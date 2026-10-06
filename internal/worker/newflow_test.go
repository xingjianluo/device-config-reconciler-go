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

// countingClient counts actual device calls so tests can prove coalescing and
// no double dispatch under concurrency.
type countingClient struct {
	*device.Client
	mu        sync.Mutex
	applies   int
	cleanups  int
	applySpec []model.Spec
	ctx       context.Context
}

func newCountingClient() *countingClient {
	return &countingClient{Client: device.NewClient()}
}

// waitRelease blocks on release but also returns when the test context ends, so
// a blocked device call can never leak a worker goroutine on test failure.
func (c *countingClient) waitRelease(release <-chan struct{}) {
	select {
	case <-release:
	case <-c.ctx.Done():
	}
}

func (c *countingClient) Apply(dev, feature string, spec model.Spec, gen int64) error {
	err := c.Client.Apply(dev, feature, spec, gen)
	c.mu.Lock()
	c.applies++
	c.applySpec = append(c.applySpec, spec)
	c.mu.Unlock()
	return err
}

func (c *countingClient) Cleanup(dev, feature string) error {
	err := c.Client.Cleanup(dev, feature)
	c.mu.Lock()
	c.cleanups++
	c.mu.Unlock()
	return err
}

func (c *countingClient) counts() (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.applies, c.cleanups
}

func newCountingHarness(t *testing.T, cfg Config) (*Worker, *countingClient, context.CancelFunc) {
	t.Helper()
	store := storage.NewMemoryStore()
	client := newCountingClient()
	rec := reconciler.New(store, client)
	w := New(rec, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	client.ctx = ctx
	w.Start(ctx)
	return w, client, cancel
}

func putGen(id, dev string, gen int64, level int) model.Event {
	return model.Event{
		EventID: id, Type: model.EventPut, DeviceID: dev, Feature: "wifi",
		Spec: &model.Spec{Enabled: true, Mode: "auto", Level: level}, Generation: gen,
	}
}

func delGen(id, dev string, gen int64) model.Event {
	return model.Event{
		EventID: id, Type: model.EventDelete, DeviceID: dev, Feature: "wifi", Generation: gen,
	}
}

// TestOutOfOrderBurstConvergesToLastCommit submits many generations for one
// object in shuffled order (incl. duplicates), then asserts the device ends at
// the highest committed generation and processing was bounded by coalescing.
func TestOutOfOrderBurstConvergesToLastCommit(t *testing.T) {
	w, client, cancel := newCountingHarness(t, defaultCfg())
	defer func() { cancel(); w.Stop() }()

	const n = 200
	t.Logf("trigger=submit gens 1..%d for one object in shuffled order with replayed duplicates | "+
		"verdict: device at gen %d (last commit) with bounded device applies (< %d)", n, n, n)

	// Deterministic shuffle-ish order: descending even gens, then ascending odd
	// gens (so low generations arrive late), plus duplicate replays of gen1.
	// Levels stay inside the legal [0,100] range so convergence is not blocked
	// by permanent validation.
	for g := n; g >= 2; g -= 2 {
		w.Submit(putGen(fmt.Sprintf("e-%d", g), "dev", int64(g), int(g%101)))
	}
	for g := 1; g <= n; g += 2 {
		w.Submit(putGen(fmt.Sprintf("e-%d", g), "dev", int64(g), int(g%101)))
	}
	w.Submit(putGen("e-1-replay-a", "dev", 1, 1))
	w.Submit(putGen("e-1-replay-b", "dev", 1, 1))
	w.Drain()

	st, ok := client.Get("dev", "wifi")
	if !ok || st.Level != n%101 {
		t.Fatalf("device state=%+v ok=%v, want last committed level %d (late old changes must not roll it back)", st, ok, n%101)
	}
	res := w.Results()["dev/wifi"]
	if !res.Converged || res.Generation != int64(n) {
		t.Fatalf("result=%+v, want converged at gen %d", res, n)
	}
	applies, _ := client.counts()
	if applies >= n {
		t.Fatalf("device Apply calls=%d, want <%d: queued changes must be coalesced into bounded work", applies, n)
	}
	s := w.Stats()
	if s.Pending != 0 || s.Queued != 0 || s.Parked != 0 || s.InFlight != 0 {
		t.Fatalf("stats after drain=%+v, want zero backlog", s)
	}
	if s.MergedSubmissions == 0 {
		t.Fatalf("MergedSubmissions=%d, want >0 proving burst events folded behind one claim", s.MergedSubmissions)
	}
	t.Logf("verdict: converged to gen%d with only %d device applies (merged=%d), backlog=0 after drain -> PASS",
		n, applies, s.MergedSubmissions)
}

// TestLateOldEventWhileInFlightDoesNotRegress blocks the very first Apply, lets
// a newer event be coalesced behind it, and then also replays an old event.
// Only the newest intent may remain on the device.
func TestLateOldEventWhileInFlightDoesNotRegress(t *testing.T) {
	w, client, cancel := newCountingHarness(t, defaultCfg())
	defer func() { cancel(); w.Stop() }()

	inside := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	client.SetApplyHook(func(dev, feature string) {
		if dev == "dev" && feature == "wifi" {
			once.Do(func() { close(inside) })
			client.waitRelease(release)
		}
	})

	t.Log("trigger=gen1 Apply in flight; submit gen2 (coalesced) then replayed gen1 (stale) before release | " +
		"verdict: final device level=2, old replay cannot regress, same event not dispatched twice")
	go w.Submit(putGen("g1", "dev", 1, 1))
	<-inside

	// While gen1 is blocked in the device call, gen2 folds into the parked
	// slot; a duplicate gen1 EventID and a stale old gen must not regress.
	w.Submit(putGen("g2", "dev", 2, 2))
	w.Submit(putGen("g1", "dev", 1, 1))      // identical EventID -> duplicate
	w.Submit(putGen("g1-oldx", "dev", 1, 1)) // same gen, different ID, older than 2 -> stale
	close(release)
	w.Drain()

	st, ok := client.Get("dev", "wifi")
	if !ok || st.Level != 2 {
		t.Fatalf("device state=%+v ok=%v, want level 2", st, ok)
	}
	applies, _ := client.counts()
	if applies != 2 {
		t.Fatalf("Apply calls=%d, want exactly 2 (gen1 then gen2), no duplicate dispatch", applies)
	}
	t.Logf("verdict: device level=2, applies=%d (no double dispatch), stale replay acked only -> PASS", applies)
}

// TestDuplicateConcurrentEventDispatchedOnce runs the same EventID burst under
// many goroutines: exactly one device call may happen.
func TestDuplicateConcurrentEventDispatchedOnce(t *testing.T) {
	w, client, cancel := newCountingHarness(t, Config{
		Workers: 8, MaxRetries: 3, BaseBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond, MaxQueue: 4096,
	})
	defer func() { cancel(); w.Stop() }()

	t.Log("trigger=same EventID submitted 128 times from 128 goroutines | verdict: exactly 1 Apply, ledger 1")
	var wg sync.WaitGroup
	for i := 0; i < 128; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); w.Submit(putGen("dup-id", "dev", 5, 55)) }()
	}
	wg.Wait()
	w.Drain()
	applies, _ := client.counts()
	if applies != 1 {
		t.Fatalf("Apply calls=%d, want exactly 1 even under concurrency", applies)
	}
	if st, ok := client.Get("dev", "wifi"); !ok || st.Level != 55 {
		t.Fatalf("device state ok=%v level=%d, want 55", ok, st.Level)
	}
	t.Logf("verdict: exactly %d device Apply for 128 identical deliveries -> PASS", applies)
}

// TestDeleteThenRecreateInterleavedAtWorker covers PUT1, DELETE2, PUT3 fired
// back-to-back: final intent (recreate) must win with no delete residue.
func TestDeleteThenRecreateInterleavedAtWorker(t *testing.T) {
	w, client, cancel := newCountingHarness(t, defaultCfg())
	defer func() { cancel(); w.Stop() }()

	t.Log("trigger=PUT gen1, DELETE gen2, PUT gen3 submitted back-to-back | verdict: device at gen3, delete not terminal")
	w.Submit(putGen("p1", "dev", 1, 10))
	w.Submit(delGen("d2", "dev", 2))
	w.Submit(putGen("p3", "dev", 3, 33))
	w.Drain()

	st, ok := client.Get("dev", "wifi")
	if !ok || st.Level != 33 {
		t.Fatalf("device state=%+v ok=%v, want recreated level 33, no residual delete", st, ok)
	}
	res := w.Results()["dev/wifi"]
	if !res.Converged || res.Generation != 3 {
		t.Fatalf("result=%+v, want converged gen3; a superseded delete must never be the final word", res)
	}
	t.Logf("verdict: recreated gen3 level=33, result converged gen%d -> PASS", res.Generation)
}

// TestCreateDuringInFlightDeleteCoversNewIntent blocks CLEANUP and submits a
// recreate before it finishes: the claimed delete must be superseded and the
// device must end at the new spec without a delete completion being reported.
func TestCreateDuringInFlightDeleteCoversNewIntent(t *testing.T) {
	w, client, cancel := newCountingHarness(t, defaultCfg())
	defer func() { cancel(); w.Stop() }()

	w.Submit(putGen("p1", "dev", 1, 10))
	w.Drain()

	inside := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	client.SetCleanupHook(func(dev, feature string) {
		if dev == "dev" && feature == "wifi" {
			once.Do(func() { close(inside) })
			client.waitRelease(release)
		}
	})

	t.Log("trigger=DELETE cleanup blocked in flight, PUT gen3 arrives before cleanup confirmed | " +
		"verdict: delete never reported finished, final device state is gen3")
	go w.Submit(delGen("d2", "dev", 2))
	<-inside
	w.Submit(putGen("p3", "dev", 3, 88))
	close(release)
	w.Drain()

	st, ok := client.Get("dev", "wifi")
	if !ok || st.Level != 88 {
		t.Fatalf("device state=%+v ok=%v, want level 88 from recreate, no residual empty state", st, ok)
	}
	res := w.Results()["dev/wifi"]
	if !res.Converged || res.Generation != 3 || res.Action == model.ActionCleanup {
		t.Fatalf("final result=%+v, want converged gen3 non-cleanup: superseded delete must not be exposed as finished", res)
	}
	t.Logf("verdict: blocked delete superseded by gen3, device level=88, final result action=%s -> PASS", res.Action)
}

// TestBoundedQueueBackpressureAndStats verifies TrySubmit rejects beyond
// MaxQueue while claims are held, and that high-frequency writes to one object
// coalesce (never queue unboundedly); Stats exposes the live backlog.
func TestBoundedQueueBackpressureAndStats(t *testing.T) {
	const cap = 8
	w, client, cancel := newCountingHarness(t, Config{
		Workers: 1, MaxRetries: 5, BaseBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond, MaxQueue: cap,
	})
	defer func() { cancel(); w.Stop() }()

	inside := make(chan struct{})
	release := make(chan struct{})
	client.SetApplyHook(func(dev, feature string) {
		// Block every Apply: the first claim becomes in-flight while all other
		// distinct objects pile up behind it in the bounded queue.
		select {
		case <-inside:
		default:
			close(inside)
		}
		client.waitRelease(release)
	})

	t.Logf("trigger=worker blocked in device call, fill %d runnable queue slots then submit one more | "+
		"verdict: %d accepted (1 in-flight + %d queued), extra rejected with ErrQueueFull", cap+1, cap+1, cap)
	go w.Submit(putGen("g0", "dev0", 1, 0))
	// Wait until the single worker is parked inside the blocked Apply.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if w.Stats().InFlight == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("precondition: in-flight never became 1: %+v", w.Stats())
		}
		time.Sleep(time.Millisecond)
	}
	for i := 1; i <= cap; i++ {
		if err := w.TrySubmit(putGen(fmt.Sprintf("g%d", i), fmt.Sprintf("dev%d", i), 1, i)); err != nil {
			t.Fatalf("submit %d within capacity got err=%v", i, err)
		}
	}
	if err := w.TrySubmit(putGen("overflow", "devX", 1, 1)); err != ErrQueueFull {
		t.Fatalf("overflow submit err=%v, want ErrQueueFull (backpressure)", err)
	}
	s := w.Stats()
	if s.Queued != cap || s.InFlight != 1 || s.Pending != cap+1 || s.Dropped != 1 {
		t.Fatalf("stats at capacity=%+v, want queued=%d in-flight=1 pending=%d dropped=1", s, cap, cap+1)
	}
	t.Logf("verdict: bounded at %d queued slots, overflow rejected, stats={queued:%d in-flight:%d retrying:%d dropped:%d} -> PASS",
		cap, s.Queued, s.InFlight, s.Retrying, s.Dropped)

	// Same-object burst while full: must coalesce, never rejected and never
	// grow the queue beyond capacity.
	t.Log("trigger=200 more changes for one already-queued object while queue is full | " +
		"verdict: all accepted via coalescing, queue length unchanged, converges to final gen")
	for g := 2; g <= 201; g++ {
		if err := w.TrySubmit(putGen(fmt.Sprintf("dev1-g%d", g), "dev1", int64(g), g%101)); err != nil {
			t.Fatalf("same-object coalescing submit g=%d got err=%v, must not be dropped", g, err)
		}
	}
	s = w.Stats()
	if s.Queued != cap {
		t.Fatalf("queue grew to %d via same-object writes, want stays %d (bounded coalescing)", s.Queued, cap)
	}
	if s.Parked != 1 || s.MergedSubmissions != 200 {
		t.Fatalf("stats=%+v, want parked=1 merged=200", s)
	}
	close(release)
	w.Drain()
	st, ok := client.Get("dev1", "wifi")
	if !ok || st.Level != 201%101 {
		t.Fatalf("dev1 state=%+v ok=%v, want last committed level %d (coalescing must not change convergence)", st, ok, 201%101)
	}
	if s = w.Stats(); s.Pending != 0 {
		t.Fatalf("stats after drain=%+v, want pending=0", s)
	}
	t.Logf("verdict: 200 same-object writes merged into 1 parked slot (queue stays %d), converged level=%d, backlog=0 -> PASS", cap, 201%101)
}

// TestSubmitContextBackpressureUnblocksOnCancel verifies a blocked submitter
// returns promptly when its context is cancelled; after a slot frees, a fresh
// submission goes through.
func TestBoundedQueueSubmitContextCancel(t *testing.T) {
	const cap = 2
	w, client, cancel := newCountingHarness(t, Config{
		Workers: 1, MaxRetries: 1, BaseBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond, MaxQueue: cap,
	})
	defer func() { cancel(); w.Stop() }()

	release := make(chan struct{})
	client.SetApplyHook(func(dev, feature string) { client.waitRelease(release) })

	// Hold one claim in-flight and all cap runnable slots queued (cap+1 claims
	// in total): the bounded queue is full for the whole assertion.
	for i := 0; i < cap+1; i++ {
		go w.Submit(putGen(fmt.Sprintf("g%d", i), fmt.Sprintf("dev%d", i), 1, i))
	}
	deadline := time.Now().Add(2 * time.Second)
	for w.Stats().InFlight != 1 || w.Stats().Queued != cap {
		if time.Now().After(deadline) {
			t.Fatalf("precondition stats=%+v", w.Stats())
		}
		time.Sleep(time.Millisecond)
	}

	subCtx, subCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	t0 := time.Now()
	err := w.SubmitContext(subCtx, putGen("late", "devLate", 1, 1))
	subCancel()
	if err == nil || time.Since(t0) < 40*time.Millisecond {
		t.Fatalf("SubmitContext err=%v after %v, want context deadline while full", err, time.Since(t0))
	}
	t.Logf("verdict: back-pressured submit returned %v after ~%v on context cancel -> PASS", err, time.Since(t0).Round(time.Millisecond))

	close(release)
	w.Drain()
	if s := w.Stats(); s.Pending != 0 {
		t.Fatalf("stats=%+v after drain, want 0", s)
	}
}
