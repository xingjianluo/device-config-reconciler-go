package worker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xingjianluo/device-config-reconciler-go/internal/device"
	"github.com/xingjianluo/device-config-reconciler-go/internal/reconciler"
	"github.com/xingjianluo/device-config-reconciler-go/internal/storage"
)

// waitFor polls cond until it holds or the deadline (2s) passes, then fails.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

// TestQueueFullAppliesBackpressure fills a bounded queue and asserts Submit
// rejects new objects with ErrQueueFull instead of growing without bound,
// while coalesced events for in-flight objects are still accepted. The worker
// is started only after the backpressure assertions so the fill/drain pattern
// is deterministic.
func TestQueueFullAppliesBackpressure(t *testing.T) {
	cfg := Config{Workers: 2, MaxRetries: 5, BaseBackoff: time.Millisecond, MaxBackoff: 10 * time.Millisecond, MaxQueueSize: 2}
	store, client := storage.NewMemoryStore(), device.NewClient()
	w := New(reconciler.New(store, client), cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); w.Stop() }()

	t.Log("trigger=queue capacity 2, worker not yet started, 5 distinct objects submitted | verdict: 2 accepted, 3 rejected with ErrQueueFull")
	accepted, rejected := 0, 0
	for i := 0; i < 5; i++ {
		err := w.Submit(putEv(fmt.Sprintf("bp-%d", i), fmt.Sprintf("dev-%d", i), 1, i))
		switch {
		case err == nil:
			accepted++
		case errors.Is(err, ErrQueueFull):
			rejected++
		default:
			t.Fatalf("unexpected submit error: %v", err)
		}
	}
	if accepted != 2 || rejected != 3 {
		t.Fatalf("accepted=%d rejected=%d, want 2 accepted (capacity) and 3 rejected with ErrQueueFull", accepted, rejected)
	}
	stats := w.Stats()
	t.Logf("  backlog while full: %+v", stats)
	if stats.Queued != 2 {
		t.Fatalf("Stats().Queued=%d, want 2 (queue held at capacity, no unbounded growth)", stats.Queued)
	}

	t.Log("trigger=another event for an ALREADY accepted object while queue is full | verdict: accepted via coalescing slot, queue does not grow")
	if err := w.Submit(putEv("bp-0b", "dev-0", 2, 11)); err != nil {
		t.Fatalf("coalesced submit for in-flight object must not be rejected: %v", err)
	}
	if got := w.Stats().Coalesced; got != 1 {
		t.Fatalf("Stats().Coalesced=%d, want 1 (event parked in coalescing slot)", got)
	}
	if got := w.Stats().Queued; got != 2 {
		t.Fatalf("Stats().Queued=%d, want still 2: coalescing must not grow the queue", got)
	}

	t.Log("trigger=worker starts and drains, then a previously rejected event is re-submitted | verdict: accepted (rejection did not mark it seen), all accepted objects converge")
	w.Start(ctx)
	waitFor(t, "queue drains below capacity", func() bool { return w.Stats().Queued < 2 })
	if err := w.Submit(putEv("bp-3", "dev-3", 1, 3)); err != nil {
		t.Fatalf("re-submit of previously rejected event must be accepted: %v", err)
	}
	w.Drain()

	// dev-2/dev-4 were rejected and never re-submitted: only the accepted
	// objects (dev-0, dev-1, dev-3) can be expected to converge.
	for _, i := range []int{0, 1, 3} {
		key := fmt.Sprintf("dev-%d/wifi", i)
		if res, ok := w.Results()[key]; !ok || !res.Converged {
			t.Fatalf("%s result=%+v ok=%v, want converged after recovery", key, res, ok)
		}
	}
	st, _ := client.Get("dev-0", "wifi")
	if st.Level != 11 {
		t.Fatalf("dev-0 level=%d, want 11: coalesced gen2 event must be the final converged state", st.Level)
	}
	if got := w.Stats(); got.Queued != 0 || got.Retrying != 0 || got.Coalesced != 0 {
		t.Fatalf("Stats()=%+v after drain, want all zero", got)
	}
	t.Logf("verdict: backpressure rejected 3, coalesced 1, all accepted objects converged after recovery, stats back to zero -> PASS")
}

// TestCoalescingBoundsDeviceWrites buries one slow object under a burst of
// changes and asserts the pending work and the number of device operations
// stay bounded instead of growing one queue entry per change.
func TestCoalescingBoundsDeviceWrites(t *testing.T) {
	cfg := Config{Workers: 1, MaxRetries: 1000, BaseBackoff: 20 * time.Millisecond, MaxBackoff: 50 * time.Millisecond}
	w, store, client, cancel := newHarness(t, cfg)
	defer func() { cancel(); w.Stop() }()

	const burst = 50 // level == gen, always within [0,100]
	t.Logf("trigger=device locked while %d distinct-gen changes for ONE object are submitted | verdict: queue stays <=1 entry for the object, device writes bounded, final state = last gen", burst)
	client.InjectTransientFailure("dev", "wifi", 1) // first attempt fails -> long retry backoff
	mustSubmit(t, w, putEv("c-1", "dev", 1, 10))
	waitFor(t, "first event parked in retry backoff", func() bool { return w.Stats().Retrying == 1 })

	for i := 2; i <= burst; i++ {
		mustSubmit(t, w, putEv(fmt.Sprintf("c-%d", i), "dev", int64(i), i))
	}
	stats := w.Stats()
	t.Logf("  backlog during burst: %+v", stats)
	if stats.Coalesced != 1 {
		t.Fatalf("Stats().Coalesced=%d, want exactly 1: %d changes must collapse into a single parked event", stats.Coalesced, burst-1)
	}
	if stats.Queued != 0 {
		t.Fatalf("Stats().Queued=%d, want 0: coalesced events must not occupy queue entries", stats.Queued)
	}

	client.ResetFailures("dev", "wifi")
	w.Drain()

	st, ok := client.Get("dev", "wifi")
	if !ok || st.Level != burst {
		t.Fatalf("device state=%+v ok=%v, want level %d (the last submitted change)", st, ok, burst)
	}
	if l := store.Ledger("dev", "wifi"); len(l) == 0 || len(l) > 3 || l[len(l)-1].Generation != burst {
		t.Fatalf("ledger=%+v, want a short ledger ending at gen%d (coalesced burst)", l, burst)
	}
	// Applies: 1 failed attempt + a small constant number of real ones, NOT 200.
	if got := client.ApplyCount("dev", "wifi"); got > 5 {
		t.Fatalf("device Apply issued %d times for %d changes, want <= 5 (coalescing bounds device work)", got, burst)
	}
	t.Logf("verdict: %d changes -> %d device applies, final level=%d, ledger=[gen%d] -> bounded work, unchanged convergence -> PASS",
		burst, client.ApplyCount("dev", "wifi"), st.Level, burst)
}

// TestStatsExposeBacklog verifies the externally visible backlog counters
// reflect queued and retrying events and return to zero after draining.
func TestStatsExposeBacklog(t *testing.T) {
	cfg := Config{Workers: 2, MaxRetries: 100, BaseBackoff: 30 * time.Millisecond, MaxBackoff: 60 * time.Millisecond, MaxQueueSize: 10}
	w, _, client, cancel := newHarness(t, cfg)
	defer func() { cancel(); w.Stop() }()

	t.Log("trigger=3 objects whose devices are locked (transient) | verdict: Stats shows them as in-flight/retrying while they wait")
	for i := 0; i < 3; i++ {
		dev := fmt.Sprintf("dev-%d", i)
		client.InjectTransientFailure(dev, "wifi", 2)
		mustSubmit(t, w, putEv(fmt.Sprintf("s-%d", i), dev, 1, i))
	}
	waitFor(t, "at least one event parked in retry backoff", func() bool {
		return w.Stats().Retrying > 0
	})
	stats := w.Stats()
	t.Logf("  observed backlog: %+v", stats)
	if stats.Inflight != 3 {
		t.Fatalf("Stats().Inflight=%d, want 3 (all objects have unfinished work)", stats.Inflight)
	}

	w.Drain()
	stats = w.Stats()
	if stats.Queued != 0 || stats.Inflight != 0 || stats.Retrying != 0 || stats.Coalesced != 0 {
		t.Fatalf("Stats()=%+v after drain, want all zero (no hidden backlog)", stats)
	}
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("dev-%d/wifi", i)
		if res := w.Results()[key]; !res.Converged {
			t.Fatalf("%s result=%+v, want converged", key, res)
		}
	}
	t.Log("verdict: backlog visible while pending, all counters zero after drain, all converged -> PASS")
}
