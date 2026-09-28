package reconciler

import (
	"context"
	"testing"

	"github.com/xingjianluo/device-config-reconciler-go/internal/device"
	"github.com/xingjianluo/device-config-reconciler-go/internal/model"
	"github.com/xingjianluo/device-config-reconciler-go/internal/storage"
)

func spec(enabled bool, mode string, level int) *model.Spec {
	return &model.Spec{Enabled: enabled, Mode: mode, Level: level}
}

func putEvent(id string, dev, feature string, gen int64, s *model.Spec) model.Event {
	return model.Event{EventID: id, Type: model.EventPut, DeviceID: dev, Feature: feature, Spec: s, Generation: gen}
}

func deleteEvent(id, dev, feature string) model.Event {
	return model.Event{EventID: id, Type: model.EventDelete, DeviceID: dev, Feature: feature}
}

func TestCreateThenUpdateConverge(t *testing.T) {
	store, client := storage.NewMemoryStore(), device.NewClient()
	rec := New(store, client)
	ctx := context.Background()

	t.Log("trigger=PUT gen1 for a feature with no device state | verdict: CREATE action, converged, state appears")
	res := rec.Process(ctx, putEvent("e1", "dev", "wifi", 1, spec(true, "auto", 10)))
	if !res.Converged || res.Action != model.ActionCreate || res.Generation != 1 {
		t.Fatalf("res = %+v, want converged CREATE gen1", res)
	}
	st, ok := client.Get("dev", "wifi")
	if !ok || st.Level != 10 {
		t.Fatalf("device state = %+v ok=%v, want level 10", st, ok)
	}
	t.Logf("verdict: %s converged, device level=%d -> PASS", res.Action, st.Level)

	t.Log("trigger=PUT gen2 with changed spec | verdict: UPDATE action, state follows new spec")
	res = rec.Process(ctx, putEvent("e2", "dev", "wifi", 2, spec(false, "manual", 42)))
	if !res.Converged || res.Action != model.ActionUpdate || res.Generation != 2 {
		t.Fatalf("res = %+v, want converged UPDATE gen2", res)
	}
	st, _ = client.Get("dev", "wifi")
	if st.Enabled || st.Mode != "manual" || st.Level != 42 {
		t.Fatalf("device state = %+v, want disabled/manual/42", st)
	}
	t.Logf("verdict: %s converged, device = enabled=%v mode=%s level=%d -> PASS",
		res.Action, st.Enabled, st.Mode, st.Level)
}

func TestDuplicateEventIsIdempotent(t *testing.T) {
	store, client := storage.NewMemoryStore(), device.NewClient()
	rec := New(store, client)
	ctx := context.Background()
	ev := putEvent("dup-1", "dev", "vpn", 7, spec(true, "auto", 5))

	t.Log("trigger=process the SAME event object 5 times | verdict: one device CREATE, four no-op results, ledger len 1")
	var first model.ActionResult
	for i := 0; i < 5; i++ {
		r := rec.Process(ctx, ev)
		if i == 0 {
			first = r
		}
	}
	if first.Action != model.ActionCreate || !first.Converged {
		t.Fatalf("first processing = %+v, want CREATE converged", first)
	}
	if n := rec.LedgerLength("dev", "vpn"); n != 1 {
		t.Fatalf("ledger len=%d, want 1: generation must be recorded exactly once", n)
	}
	st, _ := client.Get("dev", "vpn")
	if st.Level != 5 {
		t.Fatalf("device level=%d, want 5", st.Level)
	}
	t.Logf("verdict: ledger len=1, device level=%d, identical outcome to single delivery -> PASS", st.Level)
}

func TestDriftCorrection(t *testing.T) {
	store, client := storage.NewMemoryStore(), device.NewClient()
	rec := New(store, client)
	ctx := context.Background()

	rec.Process(ctx, putEvent("e1", "dev", "wifi", 1, spec(true, "auto", 10)))
	t.Log("trigger: hand-edit device state out from under the reconciler (simulate manual drift)")
	// Simulate an out-of-band manual change on the device.
	if err := client.Apply("dev", "wifi", model.Spec{Enabled: true, Mode: "manual", Level: 99}, 0); err != nil {
		t.Fatal(err)
	}
	st, _ := client.Get("dev", "wifi")
	t.Logf("observed drift: device mode=%s level=%d while desired is auto/10", st.Mode, st.Level)

	t.Log("trigger=RESYNC (next correction round) | verdict: UPDATE restores desired spec")
	res := rec.Resync(ctx, "dev", "wifi")
	if !res.Converged || res.Action != model.ActionUpdate {
		t.Fatalf("resync result = %+v, want converged UPDATE", res)
	}
	st, _ = client.Get("dev", "wifi")
	if st.Mode != "auto" || st.Level != 10 {
		t.Fatalf("after resync state = %+v, want auto/10", st)
	}
	t.Logf("verdict: drift corrected, device back to mode=%s level=%d -> PASS", st.Mode, st.Level)

	t.Log("trigger=another RESYNC with no drift | verdict: no-op, no extra ledger entry")
	res = rec.Resync(ctx, "dev", "wifi")
	if res.Action != model.ActionNone || !res.Converged {
		t.Fatalf("stable resync = %+v, want no-op converged", res)
	}
	if n := rec.LedgerLength("dev", "wifi"); n != 2 {
		t.Fatalf("ledger len=%d, want 2 (create + correction only)", n)
	}
	t.Log("verdict: stable resync issues nothing -> PASS")
}

func TestDeleteWaitsForCleanup(t *testing.T) {
	store, client := storage.NewMemoryStore(), device.NewClient()
	rec := New(store, client)
	ctx := context.Background()

	rec.Process(ctx, putEvent("e1", "dev", "wifi", 1, spec(true, "auto", 10)))

	t.Log("trigger=DELETE while cleanup cannot finish (permanent device failure) | verdict: not converged, device state remains")
	client.InjectPermanentFailure("dev", "wifi")
	res := rec.Process(ctx, deleteEvent("d1", "dev", "wifi"))
	if res.Converged || res.ErrClass != model.ErrorClassPermanent || res.Action != model.ActionCleanup {
		t.Fatalf("delete result = %+v, want non-converged PERMANENT CLEANUP", res)
	}
	if _, ok := client.Get("dev", "wifi"); !ok {
		t.Fatal("device state must remain until cleanup actually completes")
	}
	if _, ok := store.GetDesired("dev", "wifi"); ok {
		t.Fatal("desired config should be removed even if cleanup is pending")
	}
	t.Logf("verdict: delete not finished, reason=%q; desired gone but device still present -> PASS", res.Reason)

	t.Log("trigger=device recovers, DELETE redelivered | verdict: cleanup completes, converged, state+ledger cleared")
	// Clear the injected failure by rebuilding the device would lose state;
	// instead use a new client sharing state is not supported, so exercise the
	// transient path here: remove permanent injection via dedicated test hook.
	client.ResetFailures("dev", "wifi")
	res = rec.Process(ctx, deleteEvent("d1", "dev", "wifi"))
	if !res.Converged || res.Action != model.ActionCleanup {
		t.Fatalf("second delete = %+v, want converged CLEANUP", res)
	}
	if _, ok := client.Get("dev", "wifi"); ok {
		t.Fatal("device state must be gone after successful cleanup")
	}
	if n := rec.LedgerLength("dev", "wifi"); n != 0 {
		t.Fatalf("ledger len=%d, want 0 after delete finishes", n)
	}
	t.Log("verdict: deletion finished only after cleanup, state and ledger cleared -> PASS")

	t.Log("trigger=DELETE again after completion | verdict: idempotent no-op cleanup, still converged")
	res = rec.Process(ctx, deleteEvent("d1", "dev", "wifi"))
	if !res.Converged || res.Action != model.ActionCleanup {
		t.Fatalf("duplicate delete = %+v, want converged CLEANUP", res)
	}
}

func TestPermanentValidationStopsImmediately(t *testing.T) {
	store, client := storage.NewMemoryStore(), device.NewClient()
	rec := New(store, client)
	ctx := context.Background()

	t.Log("trigger=PUT with illegal mode and out-of-range level | verdict: PERMANENT, nothing sent to device")
	res := rec.Process(ctx, putEvent("bad-1", "dev", "wifi", 1, spec(true, "weird", 150)))
	if res.Converged || res.ErrClass != model.ErrorClassPermanent {
		t.Fatalf("res = %+v, want non-converged PERMANENT", res)
	}
	if _, ok := client.Get("dev", "wifi"); ok {
		t.Fatal("invalid spec must never be applied to device")
	}
	if res.Reason == "" {
		t.Fatal("permanent failure must record an explicit reason")
	}
	t.Logf("verdict: PERMANENT with reason=%q, device untouched -> PASS", res.Reason)

	t.Log("trigger=valid update afterwards | verdict: converges once payload is legal")
	res = rec.Process(ctx, putEvent("good-1", "dev", "wifi", 2, spec(true, "auto", 50)))
	if !res.Converged {
		t.Fatalf("valid PUT = %+v, want converged", res)
	}
	t.Log("verdict: legal payload converges -> PASS")
}
