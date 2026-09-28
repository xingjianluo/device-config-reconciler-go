package worker

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/device-config-reconciler-go/internal/device"
	"github.com/xingjianluo/device-config-reconciler-go/internal/model"
	"github.com/xingjianluo/device-config-reconciler-go/internal/reconciler"
	"github.com/xingjianluo/device-config-reconciler-go/internal/storage"
)

func newHarness(t *testing.T, cfg Config) (*Worker, *storage.MemoryStore, *device.Client, context.CancelFunc) {
	t.Helper()
	store, client := storage.NewMemoryStore(), device.NewClient()
	rec := reconciler.New(store, client)
	w := New(rec, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	return w, store, client, cancel
}

func defaultCfg() Config {
	return Config{Workers: 4, MaxRetries: 5, BaseBackoff: time.Millisecond, MaxBackoff: 10 * time.Millisecond}
}

func putEv(id, dev string, gen int64, level int) model.Event {
	return model.Event{
		EventID: id, Type: model.EventPut, DeviceID: dev, Feature: "wifi",
		Spec: &model.Spec{Enabled: true, Mode: "auto", Level: level}, Generation: gen,
	}
}

func TestDuplicateEventProcessedOnce(t *testing.T) {
	w, store, _, cancel := newHarness(t, defaultCfg())
	defer func() { cancel(); w.Stop() }()

	t.Log("trigger=submit the same EventID 100 times from many goroutines | verdict: result exists once, ledger len 1")
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); w.Submit(putEv("same-id", "dev", 1, 5)) }()
	}
	wg.Wait()
	w.Drain()

	res, ok := w.Results()["dev/wifi"]
	if !ok || !res.Converged {
		t.Fatalf("result=%+v ok=%v, want single converged result", res, ok)
	}
	// Reconciler ledger is accessible through store.
	if l := store.Ledger("dev", "wifi"); len(l) != 1 {
		t.Fatalf("ledger len=%d, want 1: duplicate deliveries must equal one delivery", len(l))
	}
	t.Logf("verdict: one converged result (action=%s), ledger=1 -> PASS", res.Action)
}

func TestTransientFailureRetriedUntilSuccess(t *testing.T) {
	cfg := defaultCfg()
	w, _, client, cancel := newHarness(t, cfg)
	defer func() { cancel(); w.Stop() }()

	client.InjectTransientFailure("dev", "wifi", 3)
	t.Log("trigger=device locked for the first 3 attempts | verdict: backoff retries, converged success, attempts>=4, event never lost")
	w.Submit(putEv("ev-1", "dev", 1, 9))
	w.Drain()

	res := w.Results()["dev/wifi"]
	if !res.Converged {
		t.Fatalf("res=%+v, want eventual convergence after retries", res)
	}
	if res.Attempt < 4 {
		t.Fatalf("attempts=%d, want >=4 (1 initial + 3 transient failures)", res.Attempt)
	}
	if res.ErrClass != model.ErrorClassNone {
		t.Fatalf("final ErrClass=%q, want none on success", res.ErrClass)
	}
	st, ok := client.Get("dev", "wifi")
	if !ok || st.Level != 9 {
		t.Fatalf("device state=%+v ok=%v, want level 9 once lock cleared", st, ok)
	}
	t.Logf("verdict: converged after %d attempts with exponential backoff -> PASS", res.Attempt)
}

func TestTransientRetriesExhaustedIsRetainedClass(t *testing.T) {
	cfg := Config{Workers: 2, MaxRetries: 2, BaseBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond}
	w, _, client, cancel := newHarness(t, cfg)
	defer func() { cancel(); w.Stop() }()

	client.InjectTransientFailure("dev", "wifi", 1000)
	t.Log("trigger=device stays locked, only 2 retries allowed | verdict: terminal result is still classed TRANSIENT, never mislabeled permanent")
	w.Submit(putEv("ev-1", "dev", 1, 1))
	w.Drain()

	res := w.Results()["dev/wifi"]
	if res.Converged {
		t.Fatal("must not converge while device is locked")
	}
	if res.ErrClass != model.ErrorClassTransient {
		t.Fatalf("ErrClass=%q, want TRANSIENT so retry exhaustion stays distinguishable from bad input", res.ErrClass)
	}
	if res.Attempt != 3 {
		t.Fatalf("attempts=%d, want 3 (initial + 2 retries)", res.Attempt)
	}
	t.Logf("verdict: exhausted retries reported as TRANSIENT reason=%q attempts=%d -> PASS", res.Reason, res.Attempt)
}

func TestPermanentFailureNotRetried(t *testing.T) {
	w, _, _, cancel := newHarness(t, defaultCfg())
	defer func() { cancel(); w.Stop() }()

	t.Log("trigger=illegal spec (level 999) | verdict: stopped on attempt 1 with PERMANENT and explicit reason")
	w.Submit(putEv("bad", "dev", 1, 999))
	w.Drain()

	res := w.Results()["dev/wifi"]
	if res.Converged || res.ErrClass != model.ErrorClassPermanent {
		t.Fatalf("res=%+v, want non-converged PERMANENT", res)
	}
	if res.Attempt != 1 {
		t.Fatalf("attempts=%d, want 1: permanent failures must never be retried", res.Attempt)
	}
	if res.Reason == "" {
		t.Fatal("permanent result must carry an explicit reason")
	}
	t.Logf("verdict: attempt=1, PERMANENT reason=%q, distinct from transient path -> PASS", res.Reason)
}

func TestEndToEndDeleteWaitsForCleanup(t *testing.T) {
	w, _, client, cancel := newHarness(t, defaultCfg())
	defer func() { cancel(); w.Stop() }()

	w.Submit(putEv("put-1", "dev", 1, 11))
	w.Drain()
	t.Log("trigger=object converged, then DELETE arrives while device cleanup is transiently failing twice")
	client.InjectTransientFailure("dev", "wifi", 2)
	w.Submit(model.Event{EventID: "del-1", Type: model.EventDelete, DeviceID: "dev", Feature: "wifi"})
	w.Drain()

	res := w.Results()["dev/wifi"]
	if !res.Converged || res.Action != model.ActionCleanup {
		t.Fatalf("delete result=%+v, want converged CLEANUP after retries", res)
	}
	if _, ok := client.Get("dev", "wifi"); ok {
		t.Fatal("device state must be removed when deletion finishes")
	}
	if res.Attempt < 3 {
		t.Fatalf("delete attempts=%d, want >=3 showing cleanup was retried not abandoned", res.Attempt)
	}
	t.Logf("verdict: cleanup retried %d times then completed, device cleared -> PASS", res.Attempt)
}

func TestManyObjectsConcurrently(t *testing.T) {
	w, _, client, cancel := newHarness(t, Config{
		Workers: 8, MaxRetries: 3, BaseBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond,
	})
	defer func() { cancel(); w.Stop() }()

	const n = 100
	t.Logf("trigger=%d objects submitted concurrently, half with 1 transient failure | verdict: all converge, levels correct", n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		dev := fmt.Sprintf("dev-%d", i)
		if i%2 == 0 {
			client.InjectTransientFailure(dev, "wifi", 1)
		}
		wg.Add(1)
		go func(i int) { defer wg.Done(); w.Submit(putEv(fmt.Sprintf("e-%d", i), dev, 1, i)) }(i)
	}
	wg.Wait()
	w.Drain()

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("dev-%d/wifi", i)
		res := w.Results()[key]
		if !res.Converged {
			t.Fatalf("%s did not converge: %+v", key, res)
		}
		st, ok := client.Get(fmt.Sprintf("dev-%d", i), "wifi")
		if !ok || st.Level != i {
			t.Fatalf("%s device state=%+v ok=%v, want level %d", key, st, ok, i)
		}
	}
	t.Logf("verdict: all %d objects converged under 8 workers with race detector -> PASS", n)
}
