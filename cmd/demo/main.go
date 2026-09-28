// Command demo walks through the reconciliation semantics on the local
// in-memory simulation: create/update convergence, duplicate-event idempotency,
// drift correction, transient retry with backoff, permanent failure handling
// and deferred delete cleanup.
package main

import (
	"context"
	"fmt"

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
	cfg := worker.Config{Workers: 4, MaxRetries: 5, BaseBackoff: 1, MaxBackoff: 10}
	w := worker.New(rec, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	defer func() { cancel(); w.Stop() }()

	put := func(id, dev string, gen, level int64) {
		w.Submit(model.Event{
			EventID: id, Type: model.EventPut, DeviceID: dev, Feature: "wifi",
			Spec: &model.Spec{Enabled: true, Mode: "auto", Level: int(level)}, Generation: gen,
		})
	}
	del := func(id, dev string) {
		w.Submit(model.Event{EventID: id, Type: model.EventDelete, DeviceID: dev, Feature: "wifi"})
	}
	show := func(tag, dev string) {
		r := w.Results()[dev+"/wifi"]
		st, ok := client.Get(dev, "wifi")
		fmt.Printf("[%s] result={action:%s converged:%v class:%q attempts:%d reason:%q} device={present:%v level:%d}\n",
			tag, r.Action, r.Converged, r.ErrClass, r.Attempt, r.Reason, ok, st.Level)
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

	// 4. transient failure with backoff retry
	client.InjectTransientFailure("dev-B", "wifi", 3)
	put("p3", "dev-B", 1, 30)
	w.Drain()
	show("transient    ", "dev-B")

	// 5. permanent failure is not retried
	put("p4", "dev-C", 1, 999) // level out of [0,100]
	w.Drain()
	show("permanent    ", "dev-C")

	// 6. delete must wait for cleanup
	put("p5", "dev-D", 1, 40)
	w.Drain()
	client.InjectTransientFailure("dev-D", "wifi", 2)
	del("d1", "dev-D")
	w.Drain()
	show("delete       ", "dev-D")
}
