package worker

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/xingjianluo/device-config-reconciler-go/internal/model"
)

// TestOutOfOrderFloodConvergesToLastCommit sprays many generations of one
// object at the worker concurrently, in shuffled order and with duplicate
// deliveries, and asserts the device converges to the highest committed
// generation with bounded device writes.
func TestOutOfOrderFloodConvergesToLastCommit(t *testing.T) {
	w, store, client, cancel := newHarness(t, defaultCfg())
	defer func() { cancel(); w.Stop() }()

	const commits = 30 // level == gen, always within [0,100]
	const dupFactor = 3 // each commit is delivered dupFactor times
	t.Logf("trigger=%d commits (gen1..%d) delivered x%d from concurrent goroutines in random order | verdict: device converges to gen%d, ledger=1, applies bounded", commits, commits, dupFactor, commits)

	order := rand.Perm(commits)
	var wg sync.WaitGroup
	for _, g := range order {
		gen := int64(g + 1)
		for d := 0; d < dupFactor; d++ {
			wg.Add(1)
			go func(gen int64, d int) {
				defer wg.Done()
				id := fmt.Sprintf("flood-g%d", gen) // same ID per commit: replays dedupe
				if err := w.Submit(putEv(id, "dev", gen, int(gen))); err != nil {
					t.Errorf("submit gen=%d: %v", gen, err)
				}
			}(gen, d)
		}
	}
	wg.Wait()
	w.Drain()

	st, ok := client.Get("dev", "wifi")
	if !ok || st.Level != commits {
		t.Fatalf("device state=%+v ok=%v, want level %d (the last commit)", st, ok, commits)
	}
	// Coalescing bounds but does not eliminate intermediate applies: the
	// ledger must be short, strictly increasing, and end at the last commit.
	l := store.Ledger("dev", "wifi")
	if len(l) == 0 || l[len(l)-1].Generation != commits {
		t.Fatalf("ledger=%+v, want last entry at gen%d", l, commits)
	}
	if len(l) > 10 {
		t.Fatalf("ledger len=%d, want <= 10: %d commits x%d deliveries must collapse into bounded work", len(l), commits, dupFactor)
	}
	for i := 1; i < len(l); i++ {
		if l[i].Generation <= l[i-1].Generation {
			t.Fatalf("ledger generations not strictly increasing: %+v (a stale generation was applied after a newer one)", l)
		}
	}
	res := w.Results()["dev/wifi"]
	if !res.Converged {
		t.Fatalf("final result=%+v, want converged", res)
	}
	// Coalescing + generation guard must keep device writes far below the
	// number of submitted events (commits*dupFactor).
	if got, limit := client.ApplyCount("dev", "wifi"), commits; got > limit {
		t.Fatalf("device Apply issued %d times for %d commits x%d deliveries, want <= %d (bounded coalescing)",
			got, commits, dupFactor, limit)
	}
	t.Logf("verdict: device level=%d, ledger=[gen%d], applies=%d for %d submitted events -> converged to last commit with bounded work -> PASS",
		st.Level, commits, client.ApplyCount("dev", "wifi"), commits*dupFactor)
}

// TestDeleteRecreateInterleaved drives delete/recreate sequences through the
// queue without draining in between, and asserts the final state depends only
// on the last submitted intent.
func TestDeleteRecreateInterleaved(t *testing.T) {
	w, _, client, cancel := newHarness(t, defaultCfg())
	defer func() { cancel(); w.Stop() }()

	t.Log("trigger=PUT gen1, DELETE, PUT gen2 submitted back-to-back (no drain between) | verdict: final state = gen2 created, no residue")
	mustSubmit(t, w, putEv("i-put1", "devA", 1, 10))
	mustSubmit(t, w, model.Event{EventID: "i-del1", Type: model.EventDelete, DeviceID: "devA", Feature: "wifi"})
	mustSubmit(t, w, putEv("i-put2", "devA", 2, 20))
	w.Drain()

	st, ok := client.Get("devA", "wifi")
	if !ok || st.Level != 20 {
		t.Fatalf("devA state=%+v ok=%v, want level 20: last intent was create gen2", st, ok)
	}
	if res := w.Results()["devA/wifi"]; !res.Converged {
		t.Fatalf("devA result=%+v, want converged", res)
	}
	t.Logf("verdict: devA level=%d present -> last intent (create) wins -> PASS", st.Level)

	t.Log("trigger=PUT gen3 then DELETE back-to-back | verdict: device left empty, delete reported done only after cleanup")
	mustSubmit(t, w, putEv("i-put3", "devA", 3, 30))
	mustSubmit(t, w, model.Event{EventID: "i-del2", Type: model.EventDelete, DeviceID: "devA", Feature: "wifi"})
	w.Drain()

	if _, ok := client.Get("devA", "wifi"); ok {
		t.Fatal("last intent is delete: no device state may remain")
	}
	res := w.Results()["devA/wifi"]
	if !res.Converged || res.Action != model.ActionCleanup {
		t.Fatalf("delete result=%+v, want converged CLEANUP (confirmed only after cleanup)", res)
	}
	t.Log("verdict: device empty, result=CLEANUP converged -> last intent (delete) wins -> PASS")
}

// TestRecreateArrivesDuringPendingDeleteCleanup covers a create/update landing
// while a delete's cleanup is still being retried: the delete must not be
// reported done early, and the final state must follow the recreate.
func TestRecreateArrivesDuringPendingDeleteCleanup(t *testing.T) {
	cfg := defaultCfg()
	w, _, client, cancel := newHarness(t, cfg)
	defer func() { cancel(); w.Stop() }()

	mustSubmit(t, w, putEv("r-put1", "devB", 1, 10))
	w.Drain()

	t.Log("trigger=DELETE with cleanup transiently failing (2 locks), then PUT gen2 arrives mid-retry | verdict: delete never reported done early, final state = gen2")
	client.InjectTransientFailure("devB", "wifi", 2)
	mustSubmit(t, w, model.Event{EventID: "r-del1", Type: model.EventDelete, DeviceID: "devB", Feature: "wifi"})
	// Let the first cleanup attempt fail so the delete is genuinely pending.
	waitFor(t, "cleanup attempted at least once", func() bool {
		return client.CleanupCount("devB", "wifi") >= 1
	})
	if res, ok := w.Results()["devB/wifi"]; ok && res.Converged && res.Action == model.ActionCleanup {
		t.Fatalf("delete reported done (%+v) before cleanup was confirmed", res)
	}
	mustSubmit(t, w, putEv("r-put2", "devB", 2, 55))
	w.Drain()

	st, ok := client.Get("devB", "wifi")
	if !ok || st.Level != 55 {
		t.Fatalf("devB state=%+v ok=%v, want level 55: recreate during pending delete must win", st, ok)
	}
	res := w.Results()["devB/wifi"]
	if !res.Converged || res.Generation != 2 {
		t.Fatalf("final result=%+v, want converged gen2", res)
	}
	t.Logf("verdict: device level=%d, final result gen=%d action=%s -> recreate superseded pending delete -> PASS",
		st.Level, res.Generation, res.Action)
}

// mustSubmit fails the test if Submit returns an error.
func mustSubmit(t *testing.T, w *Worker, ev model.Event) {
	t.Helper()
	if err := w.Submit(ev); err != nil {
		t.Fatalf("submit %s: %v", ev.EventID, err)
	}
}
