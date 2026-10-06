package reconciler

import (
	"context"
	"testing"

	"github.com/xingjianluo/device-config-reconciler-go/internal/device"
	"github.com/xingjianluo/device-config-reconciler-go/internal/model"
	"github.com/xingjianluo/device-config-reconciler-go/internal/storage"
)

func deleteEventGen(id, dev, feature string, gen int64) model.Event {
	ev := deleteEvent(id, dev, feature)
	ev.Generation = gen
	return ev
}

// TestStalePutCannotRollBackNewerGeneration feeds generations in reverse order
// (gen3 already committed, then a late gen2/gen1 arrive). The device must stay
// at the newest intent and no extra ledger entry may be recorded.
func TestStalePutCannotRollBackNewerGeneration(t *testing.T) {
	store, client := storage.NewMemoryStore(), device.NewClient()
	rec := New(store, client)
	ctx := context.Background()

	rec.Process(ctx, putEvent("e3", "dev", "wifi", 3, spec(true, "auto", 30)))
	t.Log("trigger=gen3 already converged, then late gen2 and gen1 PUTs arrive out of order | " +
		"verdict: stale acks only, device stays at gen3 level 30, ledger len 1")
	for g, level := range map[int64]int{2: 20, 1: 10} {
		r := rec.Process(ctx, putEvent("late", "dev", "wifi", g, spec(true, "auto", level)))
		if !r.Stale || !r.Converged {
			t.Fatalf("gen%d result=%+v, want stale converged acknowledgement", g, r)
		}
	}
	st, ok := client.Get("dev", "wifi")
	if !ok || st.Level != 30 {
		t.Fatalf("device state=%+v ok=%v, want newest intent level 30 (old changes must not roll back)", st, ok)
	}
	if n := rec.LedgerLength("dev", "wifi"); n != 1 {
		t.Fatalf("ledger len=%d, want 1: stale events are not applied nor recorded", n)
	}
	if res, ok := store.GetResult("dev", "wifi"); !ok || res.Generation != 3 {
		t.Fatalf("latest result gen=%v ok=%v, want gen3 not overwritten by stale acks", res.Generation, ok)
	}
	t.Log("verdict: late gen2/gen1 acknowledged stale, device remains level 30, ledger=1 -> PASS")
}

// TestStalePutAfterDeleteCannotResurrect covers a delete followed (after
// cleanup) by a duplicated/late old PUT: the object must stay gone.
func TestStalePutAfterDeleteCannotResurrect(t *testing.T) {
	store, client := storage.NewMemoryStore(), device.NewClient()
	rec := New(store, client)
	ctx := context.Background()

	rec.Process(ctx, putEvent("e1", "dev", "wifi", 1, spec(true, "auto", 10)))
	rec.Process(ctx, deleteEventGen("d1", "dev", "wifi", 2))
	t.Log("trigger=PUT gen1 then DELETE gen2 completed, then a duplicated old PUT gen1 is replayed | " +
		"verdict: stale ack, device stays empty, no desired resurrected")
	if _, ok := client.Get("dev", "wifi"); ok {
		t.Fatal("precondition: device state must be gone after gen2 delete")
	}
	r := rec.Process(ctx, putEvent("e1", "dev", "wifi", 1, spec(true, "auto", 10)))
	if !r.Stale {
		t.Fatalf("late PUT after delete result=%+v, want stale", r)
	}
	if _, ok := client.Get("dev", "wifi"); ok {
		t.Fatal("stale PUT must not resurrect a deleted object on the device")
	}
	if _, ok := store.GetDesired("dev", "wifi"); ok {
		t.Fatal("stale PUT must not resurrect the desired config")
	}
	t.Log("verdict: old PUT after delete acknowledged stale, object stays deleted -> PASS")
}

// TestDeleteFailureThenRecreateWins covers a DELETE whose cleanup cannot
// finish yet, followed while it is still pending by a new PUT: the recreate
// must converge to the new spec and must not report the earlier delete as
// finished; a redelivered old DELETE must then be stale.
func TestDeleteFailureThenRecreateWins(t *testing.T) {
	store, client := storage.NewMemoryStore(), device.NewClient()
	rec := New(store, client)
	ctx := context.Background()

	rec.Process(ctx, putEvent("e1", "dev", "wifi", 1, spec(true, "auto", 10)))
	client.InjectPermanentFailure("dev", "wifi")
	del := rec.Process(ctx, deleteEventGen("d2", "dev", "wifi", 2))
	t.Log("trigger=DELETE gen2 cleanup blocked (permanent), then recreate PUT gen3 | " +
		"verdict: delete not finished, PUT gen3 converges after device recovers, no stale cleanup")
	if del.Converged {
		t.Fatalf("blocked delete=%+v, want non-converged", del)
	}

	// New intent arrives before the failed cleanup is confirmed: it is newer
	// than gen2 and must be persisted; the apply still hits the device failure.
	r := rec.Process(ctx, putEvent("e3", "dev", "wifi", 3, spec(false, "manual", 66)))
	if r.Converged {
		t.Fatalf("recreate while device failing=%+v, want non-converged but desired gen3 persisted", r)
	}
	if cfg, ok := store.GetDesired("dev", "wifi"); !ok || cfg.Generation != 3 {
		t.Fatalf("desired=%+v ok=%v, want gen3 persisted even while delete was pending", cfg, ok)
	}
	// A redelivered old DELETE must not undo the recreate intent.
	r = rec.Process(ctx, deleteEventGen("d2-redeliver", "dev", "wifi", 2))
	if !r.Stale {
		t.Fatalf("redelivered gen2 delete result=%+v, want stale against gen3", r)
	}
	if _, ok := store.GetDesired("dev", "wifi"); !ok {
		t.Fatal("stale delete must not remove the gen3 desired config")
	}

	client.ResetFailures("dev", "wifi")
	r = rec.Process(ctx, putEvent("e3", "dev", "wifi", 3, spec(false, "manual", 66)))
	if !r.Converged || r.Generation != 3 {
		t.Fatalf("recreate after recovery=%+v, want converged gen3", r)
	}
	st, ok := client.Get("dev", "wifi")
	if !ok || st.Level != 66 || st.Mode != "manual" || st.Enabled {
		t.Fatalf("device state=%+v ok=%v, want disabled/manual/66", st, ok)
	}
	t.Log("verdict: pending delete superseded by gen3, redelivered gen2 stale, final device at gen3 -> PASS")
}

func TestDeleteThenRecreateInterleaved(t *testing.T) {
	store, client := storage.NewMemoryStore(), device.NewClient()
	rec := New(store, client)
	ctx := context.Background()

	rec.Process(ctx, putEvent("e1", "dev", "wifi", 1, spec(true, "auto", 10)))
	rec.Process(ctx, deleteEventGen("d2", "dev", "wifi", 2))
	t.Log("trigger=PUT gen1, DELETE gen2, then recreate PUT gen3 | verdict: converges to gen3, no residual delete state")
	r := rec.Process(ctx, putEvent("e3", "dev", "wifi", 3, spec(false, "manual", 77)))
	if !r.Converged || r.Generation != 3 {
		t.Fatalf("recreate result=%+v, want converged gen3", r)
	}
	st, ok := client.Get("dev", "wifi")
	if !ok || st.Level != 77 || st.Enabled || st.Mode != "manual" {
		t.Fatalf("device state=%+v ok=%v, want disabled/manual/77", st, ok)
	}
	// The gen2 delete must now be stale against the recreated object.
	r = rec.Process(ctx, deleteEventGen("d2-redeliver", "dev", "wifi", 2))
	if !r.Stale {
		t.Fatalf("redelivered old delete result=%+v, want stale: object was recreated at gen3", r)
	}
	if _, ok := client.Get("dev", "wifi"); !ok {
		t.Fatal("stale delete must not remove the recreated device state")
	}
	t.Log("verdict: recreated at gen3, redelivered gen2 delete is stale, state intact -> PASS")
}
