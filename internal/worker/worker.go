package worker

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/xingjianluo/device-config-reconciler-go/internal/model"
	"github.com/xingjianluo/device-config-reconciler-go/internal/reconciler"
)

// ErrQueueFull is returned by TrySubmit when the bounded runnable queue is at
// capacity and the object does not yet have an in-flight claim.
var ErrQueueFull = errors.New("worker queue full")

// queuedEvent is one claim on one object: while it lives, every later event for
// the same object is coalesced into the worker's parked slot and the claim is
// repeatedly advanced to the newest intent until no newer intent remains.
type queuedEvent struct {
	ev      model.Event
	attempt int
}

// QueueStats is a point-in-time view of the worker's backlog.
type QueueStats struct {
	// Pending is the total amount of accepted, not-yet-finished work:
	// runnable queue slots + claims parked in retry backoff + coalesced events
	// folded behind an in-flight claim.
	Pending int
	// Queued is the number of runnable claims waiting for a worker goroutine.
	Queued int
	// InFlight is the number of claims being processed right now.
	InFlight int
	// Retrying is the number of claims parked in retry backoff.
	Retrying int
	// Parked is the number of coalesced newest-intent events waiting behind an
	// in-flight claim of the same object.
	Parked int
	// InflightObjects is the number of distinct objects that currently hold a
	// claim (queued, processing or retrying).
	InflightObjects int
	// MergedSubmissions is the cumulative count of submissions that were folded
	// into an existing claim instead of occupying a queue slot.
	MergedSubmissions int
	// Dropped is the cumulative count of TrySubmit rejections because the
	// runnable queue was at capacity.
	Dropped int
}

// Worker drives reconciliation from a bounded, coalescing in-memory event
// queue.
type Worker struct {
	rec *reconciler.Reconciler
	cfg Config

	mu     sync.Mutex
	cond   *sync.Cond // signalled when a runnable queue slot frees up
	seen   map[string]bool
	claims map[string]bool // object keys currently holding a claim (queued/in-flight/retrying)
	parked map[string]model.Event
	queue  []queuedEvent
	// retrying keys are claims currently sleeping in retry backoff.
	retrying map[string]bool
	active   int // claims being processed by a worker goroutine right now
	results  map[string]model.ActionResult

	merged  int
	dropped int

	notify chan struct{}

	// pending counts live claims (one per queued/processing/retrying slot).
	// A claim that picks up a coalesced event at completion re-arms itself
	// instead of finishing, so Drain returns only after the newest intent of
	// every accepted event has reached a terminal result.
	pending sync.WaitGroup

	ctx      context.Context
	wg       sync.WaitGroup
	stopOnce sync.Once
}

// Config tunes worker execution.
type Config struct {
	Workers     int           // number of concurrent processing goroutines
	MaxRetries  int           // cap on transient retries per claim incarnation
	BaseBackoff time.Duration // first retry delay
	MaxBackoff  time.Duration // retry delay ceiling
	MaxQueue    int           // bounded number of runnable queue slots (default 1024)
}

// New constructs a worker for the given reconciler.
func New(rec *reconciler.Reconciler, cfg Config) *Worker {
	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 0
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = time.Millisecond
	}
	if cfg.MaxBackoff < cfg.BaseBackoff {
		cfg.MaxBackoff = cfg.BaseBackoff
	}
	if cfg.MaxQueue <= 0 {
		cfg.MaxQueue = 1024
	}
	w := &Worker{
		rec:      rec,
		cfg:      cfg,
		seen:     make(map[string]bool),
		claims:   make(map[string]bool),
		parked:   make(map[string]model.Event),
		retrying: make(map[string]bool),
		results:  make(map[string]model.ActionResult),
		notify:   make(chan struct{}, 1),
	}
	w.cond = sync.NewCond(&w.mu)
	return w
}

// Submit enqueues an event, blocking while the bounded queue is full. It
// returns once the event has been accepted (queued or coalesced into an
// existing claim for the same object).
func (w *Worker) Submit(ev model.Event) {
	if err := w.SubmitContext(context.Background(), ev); err != nil {
		// Only reachable when the worker is stopped; preserve the legacy
		// best-effort signature by ignoring.
	}
}

// TrySubmit enqueues an event without blocking. It returns ErrQueueFull when
// the runnable queue is at capacity and the object has no in-flight claim;
// coalescing behind an in-flight claim always succeeds and is never dropped.
func (w *Worker) TrySubmit(ev model.Event) error {
	return w.submit(ev, nil)
}

// SubmitContext enqueues an event, blocking with backpressure until a queue
// slot is free, the context is cancelled, or the worker has stopped.
func (w *Worker) SubmitContext(ctx context.Context, ev model.Event) error {
	if ctx == nil {
		ctx = context.Background()
	}
	// Wake blocked submitters if their context ends.
	stop := context.AfterFunc(ctx, func() {
		w.mu.Lock()
		w.cond.Broadcast()
		w.mu.Unlock()
	})
	defer stop()
	return w.submit(ev, ctx)
}

// submit is the common admission path. waitCtx==nil means non-blocking.
func (w *Worker) submit(ev model.Event, waitCtx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Duplicate delivery (same EventID) is accepted at most once.
	if ev.EventID != "" && w.seen[ev.EventID] {
		return nil
	}

	key := ev.Key()
	if w.claims[key] {
		// The object already holds a claim: fold the event into the parked
		// newest-intent slot. The claim re-reads it before processing, so a
		// burst of N changes occupies one bounded slot but converges to the
		// newest intent. Folding keeps the HIGHEST generation (not merely the
		// last submission), so out-of-order arrivals can never make a newer
		// commit disappear into an older one; a stale parked event is rejected
		// by generation gating inside the reconciler.
		w.foldParkedLocked(key, ev)
		w.merged++
		if ev.EventID != "" {
			w.seen[ev.EventID] = true
		}
		w.signal()
		return nil
	}

	// A fresh claim needs a runnable queue slot: apply bounded backpressure.
	for len(w.queue) >= w.cfg.MaxQueue {
		if waitCtx == nil {
			w.dropped++
			return ErrQueueFull
		}
		if err := waitCtx.Err(); err != nil {
			return err
		}
		if w.ctx != nil && w.ctx.Err() != nil {
			w.dropped++
			return ErrQueueFull
		}
		w.cond.Wait()
	}
	if waitCtx != nil && waitCtx.Err() != nil {
		return waitCtx.Err()
	}
	if w.ctx != nil && w.ctx.Err() != nil {
		w.dropped++
		return ErrQueueFull
	}

	if ev.EventID != "" {
		w.seen[ev.EventID] = true
	}
	w.claims[key] = true
	w.queue = append(w.queue, queuedEvent{ev: ev, attempt: 0})
	w.pending.Add(1)
	w.signal()
	return nil
}

// foldParkedLocked stores ev as the parked newest intent for key, keeping the
// highest generation. Events without a positive generation (legacy callers)
// follow last-write-wins, preserving the pre-versioning behavior.
func (w *Worker) foldParkedLocked(key string, ev model.Event) {
	cur, ok := w.parked[key]
	if !ok || ev.Generation <= 0 || cur.Generation <= 0 || ev.Generation >= cur.Generation {
		w.parked[key] = ev
	}
}

// Start launches the worker goroutines.
func (w *Worker) Start(ctx context.Context) {
	w.ctx = ctx
	for i := 0; i < w.cfg.Workers; i++ {
		w.wg.Add(1)
		go w.loop()
	}
}

func (w *Worker) loop() {
	defer w.wg.Done()
	for {
		item := w.pop()
		if item == nil {
			return // context cancelled and queue drained
		}
		w.mu.Lock()
		w.active++
		w.mu.Unlock()

		res := w.rec.Process(w.ctx, item.ev)
		res.Attempt = item.attempt + 1

		w.mu.Lock()
		w.active--
		retry := !res.Converged && res.ErrClass == model.ErrorClassTransient &&
			item.attempt < w.cfg.MaxRetries
		if retry {
			// Transient failure: retain the claim and retry after backoff. The
			// pending counter stays held until the chain terminates, so the
			// event cannot be lost.
			item.attempt++
			w.retrying[item.ev.Key()] = true
			go w.requeueAfter(*item, w.backoff(item.attempt))
			w.mu.Unlock()
			continue
		}
		w.mu.Unlock()

		// Terminal: converged success, permanent failure, stale ack or retries
		// exhausted. A coalesced newer event re-arms the same claim.
		w.terminate(item.ev.Key(), item.ev.Generation, res)
	}
}

// pop blocks until a runnable claim is available. The claimed event is first
// advanced to whatever newest intent was coalesced behind it. After the
// context is cancelled the queue is still drained; pop returns nil only once
// it is empty.
func (w *Worker) pop() *queuedEvent {
	w.mu.Lock()
	defer w.mu.Unlock()
	for {
		if len(w.queue) > 0 {
			item := w.queue[0]
			w.queue = w.queue[1:]
			// A slot freed: wake one back-pressured submitter.
			w.cond.Broadcast()
			// Advance the claim to the newest accepted intent, if a newer one
			// was folded in while it waited (or during a previous backoff).
			if parked, ok := w.parked[item.ev.Key()]; ok {
				delete(w.parked, item.ev.Key())
				if parked.Generation > item.ev.Generation {
					item.ev = parked
				}
				item.attempt = 0 // newest intent gets a fresh retry budget
			}
			delete(w.retrying, item.ev.Key())
			return &item
		}
		cancelled := w.ctx.Err() != nil
		if cancelled {
			return nil
		}
		w.mu.Unlock()
		select {
		case <-w.ctx.Done():
			// Re-enter the loop to drain whatever is still queued.
		case <-w.notify:
		}
		w.mu.Lock()
	}
}

// requeueAfter puts a claim back after the backoff delay. If the worker is
// stopped during the wait the claim terminates as a transient failure (but any
// coalesced newer event is still picked up by terminate's drain chain).
func (w *Worker) requeueAfter(item queuedEvent, delay time.Duration) {
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-w.ctx.Done():
		w.terminate(item.ev.Key(), item.ev.Generation, model.ActionResult{
			DeviceID:   item.ev.DeviceID,
			Feature:    item.ev.Feature,
			Generation: item.ev.Generation,
			Action:     model.ActionNone,
			ErrClass:   model.ErrorClassTransient,
			Reason:     "worker stopped before retry",
			Attempt:    item.attempt + 1,
		})
		return
	case <-t.C:
	}
	w.mu.Lock()
	delete(w.retrying, item.ev.Key())
	if w.ctx.Err() != nil {
		w.mu.Unlock()
		w.terminate(item.ev.Key(), item.ev.Generation, model.ActionResult{
			DeviceID: item.ev.DeviceID, Feature: item.ev.Feature,
			Generation: item.ev.Generation, Action: model.ActionNone,
			ErrClass: model.ErrorClassTransient, Reason: "worker stopped before retry",
			Attempt: item.attempt + 1,
		})
		return
	}
	w.queue = append(w.queue, item)
	w.mu.Unlock()
	w.signal()
}

// terminate ends one processing incarnation. If a newer event was coalesced
// behind the claim, the claim is re-armed with that event (same pending count),
// so the object keeps converging until its newest intent is handled.
func (w *Worker) terminate(key string, processedGen int64, res model.ActionResult) {
	w.mu.Lock()
	if parked, ok := w.parked[key]; ok {
		delete(w.parked, key)
		// Re-arm only if the folded intent is at least as new as the one just
		// handled; an older folded event is superseded and simply disappears.
		if parked.Generation <= 0 || processedGen <= 0 || parked.Generation >= processedGen {
			w.queue = append(w.queue, queuedEvent{ev: parked, attempt: 0})
			w.mu.Unlock()
			w.signal()
			return
		}
	}
	w.claims[key] = false
	delete(w.claims, key)
	delete(w.retrying, key)
	// Stale acknowledgements must not overwrite the object's newest result.
	if !res.Stale {
		w.results[key] = res
	}
	w.mu.Unlock()
	w.pending.Done()
}

// signal wakes one waiting worker.
func (w *Worker) signal() {
	select {
	case w.notify <- struct{}{}:
	default:
	}
}

// backoff returns an exponential delay capped at MaxBackoff.
func (w *Worker) backoff(attempt int) time.Duration {
	d := w.cfg.BaseBackoff
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= w.cfg.MaxBackoff {
			return w.cfg.MaxBackoff
		}
	}
	if d > w.cfg.MaxBackoff {
		return w.cfg.MaxBackoff
	}
	return d
}

// Drain blocks until every accepted claim has reached a terminal result,
// including chains of coalesced events and claims parked in retry backoff.
func (w *Worker) Drain() { w.pending.Wait() }

// Stop waits until queued events are drained and all workers exit. Callers must
// cancel the context first.
func (w *Worker) Stop() {
	w.stopOnce.Do(func() { w.wg.Wait() })
}

// Results returns a copy of the latest externally visible result per object.
func (w *Worker) Results() map[string]model.ActionResult {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make(map[string]model.ActionResult, len(w.results))
	for k, v := range w.results {
		out[k] = v
	}
	return out
}

// Stats returns a point-in-time snapshot of the queue backlog, useful for
// detecting buildup (queued work and retry backlog) under high write rates.
func (w *Worker) Stats() QueueStats {
	w.mu.Lock()
	defer w.mu.Unlock()
	s := QueueStats{
		Queued:            len(w.queue),
		InFlight:          w.active,
		Retrying:          len(w.retrying),
		Parked:            len(w.parked),
		InflightObjects:   len(w.claims),
		MergedSubmissions: w.merged,
		Dropped:           w.dropped,
	}
	s.Pending = s.Queued + s.InFlight + s.Retrying + s.Parked
	return s
}
