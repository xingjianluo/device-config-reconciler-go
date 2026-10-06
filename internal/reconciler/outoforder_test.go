package reconciler

import (
	"context"
	"fmt"
	"testing"

	"github.com/xingjianluo/device-config-reconciler-go/internal/device"
	"github.com/xingjianluo/device-config-reconciler-go/internal/model"
	"github.com/xingjianluo/device-config-reconciler-go/internal/storage"
)

// TestOutOfOrderPutsConvergeToLatestCommit shuffles many generations of the
// same object, including duplicates, and asserts the device converges to the
// highest committed generation regardless of arrival order.
func TestOutOfOrderPutsConvergeToLatestCommit(t *testing.T) {
	store, client := storage.NewMemoryStore(), device.NewClient()
	rec := New(store, client)
	ctx := context.Background()

	// Arrival order: newest first, then a storm of late + duplicate old ones.
	arrivals := []int64{5, 3, 1, 4, 2, 5, 3, 1}
	t.Logf("trigger=PUT generations arrive out of order %v (gen5 committed last) | verdict: device converges to gen5 and never regresses", arrivals)
	highWater := 0
	for i, gen := range arrivals {
		level := int(gen * 10) // gen N carries level N*10
		res := rec.Process(ctx, putEvent(fmt.Sprintf("oo-%d", i), "dev", "wifi", gen, spec(true, "auto", level)))
		if !res.Converged {
			t.Fatalf("arrival gen=%d result=%+v, want converged (stale arrivals must not break convergence)", gen, res)
		}
		st, _ := client.Get("dev", "wifi")
		t.Logf("  after arrival gen=%d: device level=%d (high-water so far=%d, must never regress)", gen, st.Level, highWater)
		if st.Level < highWater {
			t.Fatalf("device regressed: level=%d after stale gen=%d, was already at %d", st.Level, gen, highWater)
		}
		if st.Level > highWater {
			highWater = st.Level
		}
	}

	st, ok := client.Get("dev", "wifi")
	if !ok || st.Level != 50 {
		t.Fatalf("final device state=%+v ok=%v, want level 50 (gen5, the last commit)", st, ok)
	}
	if n := rec.LedgerLength("dev", "wifi"); n != 1 {
		t.Fatalf("ledger len=%d, want 1: only the winning generation may be recorded", n)
	}
	cfg, _ := store.GetDesired("dev", "wifi")
	if cfg.Generation != 5 {
		t.Fatalf("stored desired gen=%d, want 5: stale arrivals must not overwrite newer desired state", cfg.Generation)
	}
	t.Logf("verdict: final device level=%d, desired gen=%d, ledger=1 -> converged to last commit -> PASS",
		st.Level, cfg.Generation)
}

// TestStalePutIsNotSilentlyDropped checks that a late old generation still
// triggers convergence towards the newest known desired state, so an object
// whose newest event failed cannot be stranded at an intermediate value.
func TestStalePutIsNotSilentlyDropped(t *testing.T) {
	store, client := storage.NewMemoryStore(), device.NewClient()
	rec := New(store, client)
	ctx := context.Background()

	t.Log("trigger=gen1 converges, gen2 stored but device locked (transient) so it cannot apply yet")
	rec.Process(ctx, putEvent("s1", "dev", "wifi", 1, spec(true, "auto", 10)))
	client.InjectTransientFailure("dev", "wifi", 1)
	res := rec.Process(ctx, putEvent("s2", "dev", "wifi", 2, spec(true, "auto", 20)))
	if res.Converged || res.ErrClass != model.ErrorClassTransient {
		t.Fatalf("gen2 result=%+v, want non-converged TRANSIENT while device locked", res)
	}
	st, _ := client.Get("dev", "wifi")
	t.Logf("  device stuck at level=%d while desired gen2/level20 is stored", st.Level)

	t.Log("trigger=a STALE gen1 PUT arrives late | verdict: not dropped, re-drives convergence to gen2, device still ends at level 20")
	res = rec.Process(ctx, putEvent("s3", "dev", "wifi", 1, spec(true, "auto", 10)))
	if !res.Converged {
		t.Fatalf("stale PUT result=%+v, want converged after re-driving newest desired", res)
	}
	st, _ = client.Get("dev", "wifi")
	if st.Level != 20 {
		t.Fatalf("device level=%d, want 20: stale arrival must converge to newest desired, not to its own payload", st.Level)
	}
	if res.Generation != 2 {
		t.Fatalf("stale PUT result gen=%d, want 2 (the generation actually converged to)", res.Generation)
	}
	t.Logf("verdict: stale arrival reported gen=%d reason=%q, device level=%d -> PASS", res.Generation, res.Reason, st.Level)
}

// TestDeleteThenRecreateWhileCleanupPending covers deletion interrupted by a
// new create/update: the final state must follow the last committed intent.
func TestDeleteThenRecreateWhileCleanupPending(t *testing.T) {
	store, client := storage.NewMemoryStore(), device.NewClient()
	rec := New(store, client)
	ctx := context.Background()

	rec.Process(ctx, putEvent("e1", "dev", "wifi", 1, spec(true, "auto", 10)))

	t.Log("trigger=DELETE while cleanup is transiently blocked | verdict: delete NOT reported done, device state kept")
	client.InjectTransientFailure("dev", "wifi", 1)
	res := rec.Process(ctx, deleteEvent("d1", "dev", "wifi"))
	if res.Converged {
		t.Fatalf("delete result=%+v, must not claim completion before cleanup is confirmed", res)
	}
	if _, ok := client.Get("dev", "wifi"); !ok {
		t.Fatal("device state must remain while cleanup is unconfirmed")
	}
	t.Logf("verdict: delete pending (class=%s), device state retained -> PASS", res.ErrClass)

	t.Log("trigger=PUT gen2 arrives while the delete is still pending | verdict: last intent wins, device converges to gen2, no intermediate residue")
	res = rec.Process(ctx, putEvent("e2", "dev", "wifi", 2, spec(false, "manual", 77)))
	if !res.Converged || res.Generation != 2 {
		t.Fatalf("recreate result=%+v, want converged gen2", res)
	}
	st, ok := client.Get("dev", "wifi")
	if !ok || st.Enabled || st.Mode != "manual" || st.Level != 77 {
		t.Fatalf("device state=%+v ok=%v, want exactly the gen2 spec (disabled/manual/77)", st, ok)
	}
	if last, _ := store.LastLedger("dev", "wifi"); last.Generation != 2 {
		t.Fatalf("last ledger gen=%d, want 2", last.Generation)
	}
	t.Logf("verdict: device=enabled:%v mode:%s level:%d, ledger gen2 -> recreate superseded pending delete -> PASS",
		st.Enabled, st.Mode, st.Level)
}

// TestCreateImmediatelyFollowedByDelete covers a delete landing right after a
// create: once the dust settles nothing may remain on the device.
func TestCreateImmediatelyFollowedByDelete(t *testing.T) {
	store, client := storage.NewMemoryStore(), device.NewClient()
	rec := New(store, client)
	ctx := context.Background()

	t.Log("trigger=PUT gen1 then DELETE back-to-back | verdict: device left empty, ledger cleared, delete reported done only after cleanup")
	rec.Process(ctx, putEvent("e1", "dev", "wifi", 1, spec(true, "auto", 10)))
	res := rec.Process(ctx, deleteEvent("d1", "dev", "wifi"))
	if !res.Converged || res.Action != model.ActionCleanup {
		t.Fatalf("delete result=%+v, want converged CLEANUP", res)
	}
	if _, ok := client.Get("dev", "wifi"); ok {
		t.Fatal("last intent is delete: no device state may remain")
	}
	if n := rec.LedgerLength("dev", "wifi"); n != 0 {
		t.Fatalf("ledger len=%d, want 0 after delete finished", n)
	}
	t.Log("verdict: device empty, ledger cleared -> PASS")

	t.Log("trigger=PUT gen2 recreates the object afterwards | verdict: fresh CREATE, converges to gen2 with no residue")
	res = rec.Process(ctx, putEvent("e2", "dev", "wifi", 2, spec(true, "auto", 33)))
	if !res.Converged || res.Action != model.ActionCreate {
		t.Fatalf("recreate result=%+v, want converged CREATE", res)
	}
	st, _ := client.Get("dev", "wifi")
	if st.Level != 33 {
		t.Fatalf("device level=%d, want 33", st.Level)
	}
	t.Logf("verdict: recreated at level=%d -> PASS", st.Level)
}
