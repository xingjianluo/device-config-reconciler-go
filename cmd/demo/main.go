// Command demo walks through the reconciliation semantics on the local
// in-memory simulation: create/update convergence, duplicate-event idempotency,
// drift correction, transient retry with backoff, permanent failure handling,
// deferred delete cleanup, out-of-order convergence, delete/recreate
// interleaving and bounded-queue backpressure with backlog stats.
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/xingjianluo/device-config-reconciler-go/internal/device"
	"github.com/xingjianluo/device-config-reconciler-go/internal/model"
	"github.com/xingjianluo/device-config-reconciler-go/internal/reconciler"
	"github.com/xingjianluo/device-config-reconciler-go/internal/storage"
	"github.com/xingjianluo/device-config-reconciler-go/internal/worker"
)

func main() {
	store := storage.NewMemoryStore()
	client := device.NewClient()
	rec := reconciler.New(store, client)
	cfg := worker.Config{
		Workers: 4, MaxRetries: 5, BaseBackoff: 1, MaxBackoff: 10, MaxQueue: 64,
	}
	w := worker.New(rec, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	defer func() { cancel(); w.Stop() }()

	put := func(id, dev string, gen int64, level int) {
		w.Submit(model.Event{
			EventID: id, Type: model.EventPut, DeviceID: dev, Feature: "wifi",
			Spec: &model.Spec{Enabled: true, Mode: "auto", Level: level}, Generation: gen,
		})
	}
	del := func(id, dev string, gen int64) {
		w.Submit(model.Event{
			EventID: id, Type: model.EventDelete, DeviceID: dev, Feature: "wifi", Generation: gen,
		})
	}
	show := func(tag, dev string) {
		r := w.Results()[dev+"/wifi"]
		st, ok := client.Get(dev, "wifi")
		fmt.Printf("[%s] result={action:%s gen:%d converged:%v stale:%v class:%q attempts:%d} device={present:%v level:%d}\n",
			tag, r.Action, r.Generation, r.Converged, r.Stale, r.ErrClass, r.Attempt, ok, st.Level)
	}

	// 1. create + update convergence
	put("p1", "dev-A", 1, 10)
	w.Drain()
	show("create       ", "dev-A")
	put("p2", "dev-A", 2, 20)
	w.Drain()
	show("update       ", "dev-A")

	// 2. duplicate delivery of the same event
	put("p2", "dev-A", 2, 20) // identical EventID -> coalesced
	w.Drain()
	show("dup event    ", "dev-A")

	// 3. drift correction
	_ = client.Apply("dev-A", "wifi", model.Spec{Enabled: true, Mode: "manual", Level: 99}, 0)
	rec.Resync(ctx, "dev-A", "wifi")
	show("drift resync ", "dev-A")

	// 4. out-of-order burst: high gens first, low gens late, old gens replayed.
	for g := 10; g >= 2; g -= 2 {
		put(fmt.Sprintf("oo-%d", g), "dev-E", int64(g), g)
	}
	for g := 1; g <= 9; g += 2 {
		put(fmt.Sprintf("oo-%d", g), "dev-E", int64(g), g)
	}
	put("oo-1-replay", "dev-E", 1, 1) // duplicated old generation
	w.Drain()
	show("out-of-order ", "dev-E")
	fmt.Printf("[backlog      ] stats=%+v (merged submissions fold behind one claim)\n", w.Stats())

	// 5. delete immediately followed by recreate: last intent wins.
	put("f1", "dev-F", 1, 40)
	del("f2", "dev-F", 2)
	put("f3", "dev-F", 3, 55)
	w.Drain()
	show("delete+create", "dev-F")
	del("f2-redeliver", "dev-F", 2) // old delete replayed -> stale
	w.Drain()
	show("old delete   ", "dev-F")

	// 6. transient failure with backoff retry
	client.InjectTransientFailure("dev-B", "wifi", 3)
	put("p3", "dev-B", 1, 30)
	w.Drain()
	show("transient    ", "dev-B")

	// 7. permanent failure is not retried
	put("p4", "dev-C", 1, 999) // level out of [0,100]
	w.Drain()
	show("permanent    ", "dev-C")

	// 8. delete must wait for cleanup
	put("p5", "dev-D", 1, 40)
	w.Drain()
	client.InjectTransientFailure("dev-D", "wifi", 2)
	del("d1", "dev-D", 2)
	w.Drain()
	show("delete       ", "dev-D")

	// 9. bounded backpressure: tiny queue, blocking hook, observe live stats.
	bwCtx, bwCancel := context.WithCancel(ctx)
	bw := worker.New(rec, worker.Config{Workers: 1, MaxQueue: 2, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond})
	bw.Start(bwCtx)
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	client.SetApplyHook(func(d, f string) {
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-bwCtx.Done():
		}
	})
	bw.Submit(model.Event{EventID: "b0", Type: model.EventPut, DeviceID: "b0", Feature: "wifi",
		Spec: &model.Spec{Enabled: true, Mode: "auto", Level: 1}, Generation: 1})
	<-entered
	bw.TrySubmit(model.Event{EventID: "b1", Type: model.EventPut, DeviceID: "b1", Feature: "wifi",
		Spec: &model.Spec{Enabled: true, Mode: "auto", Level: 1}, Generation: 1})
	bw.TrySubmit(model.Event{EventID: "b2", Type: model.EventPut, DeviceID: "b2", Feature: "wifi",
		Spec: &model.Spec{Enabled: true, Mode: "auto", Level: 1}, Generation: 1})
	err := bw.TrySubmit(model.Event{EventID: "b3", Type: model.EventPut, DeviceID: "b3", Feature: "wifi",
		Spec: &model.Spec{Enabled: true, Mode: "auto", Level: 1}, Generation: 1})
	fmt.Printf("[backpressure ] third overflow TrySubmit err=%v stats=%+v\n", err, bw.Stats())
	close(release)
	bw.Drain()
	bwCancel() // unblocks the hook before its workers are stopped
	bw.Stop()
	client.SetApplyHook(nil)
	fmt.Printf("[backpressure ] after drain stats=%+v\n", bw.Stats())
}
