package worker

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/xingjianluo/device-config-reconciler-go/internal/model"
	"github.com/xingjianluo/device-config-reconciler-go/internal/reconciler"
)

// ErrQueueFull is returned by Submit when the work queue is at its configured
// capacity. It is a backpressure signal: the event was NOT accepted (and its
// EventID was not marked seen), so the caller may slow down and retry later.
var ErrQueueFull = errors.New("worker: event queue full, apply backpressure")

// queuedEvent pairs an event with its current attempt counter.
type queuedEvent struct {
	ev      model.Event
	attempt int
}

// Stats is a snapshot of the worker's backlog, exposed so operators can spot
// accumulation before it becomes a problem.
type Stats struct {
	Queued    int // events sitting in the work queue waiting for a worker
	Inflight  int // objects with an event queued, processing or parked in retry backoff
	Retrying  int // events parked in retry backoff
	Coalesced int // events parked in per-object coalescing slots (supersede-able)
}

// Worker drives reconciliation from an in-memory event queue.
//
// Backpressure and coalescing semantics:
//
//   - The queue is bounded by Config.MaxQueueSize (0 = unlimited). Submitting
//     an event for a NEW object while the queue is full fails with
//     ErrQueueFull instead of growing memory without bound.
//   - Events for an object that already has work in flight never grow the
//     queue: they are coalesced into a single per-object slot, latest
//     submission wins. This bounds the pending work per object to 2 entries
//     (1 in flight + 1 coalesced) no matter how high the write rate, without
//     changing the final convergence result (the reconciler re-reads the
//     newest desired state and guards on generations).
type Worker struct {
	rec *reconciler.Reconciler
	cfg Config

	mu        sync.Mutex
	seen      map[string]bool        // event IDs ever accepted -> duplicate suppression
	inflight  map[string]bool        // object keys currently queued/processing/retrying
	coalesced map[string]queuedEvent // object keys -> latest event parked while in flight
	queue     []queuedEvent
	results   map[string]model.ActionResult
	retrying  int // events currently parked in retry backoff
	notify    chan struct{}

	// pending counts events that have been accepted but not yet finished
	// (including events parked in a retry backoff or a coalescing slot).
	// Drain waits on it.
	pending sync.WaitGroup

	ctx      context.Context
	wg       sync.WaitGroup
	stopOnce sync.Once
}

// Config tunes worker execution.
type Config struct {
	Workers     int           // number of concurrent processing goroutines
	MaxRetries  int           // cap on transient retries per event
	BaseBackoff time.Duration // first retry delay
	MaxBackoff  time.Duration // retry delay ceiling
	// MaxQueueSize bounds how many events may sit in the work queue. Submit
	// returns ErrQueueFull once the limit is reached. 0 means unlimited
	// (backwards-compatible default).
	MaxQueueSize int
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
	if cfg.MaxQueueSize < 0 {
		cfg.MaxQueueSize = 0
	}
	return &Worker{
		rec:       rec,
		cfg:       cfg,
		seen:      make(map[string]bool),
		inflight:  make(map[string]bool),
		coalesced: make(map[string]queuedEvent),
		results:   make(map[string]model.ActionResult),
		notify:    make(chan struct{}, 1),
	}
}

// Submit enqueues an event. An event whose EventID was already submitted is a
// duplicate delivery and is coalesced: it is processed at most once, so its
// effect is identical to a single delivery (Submit reports nil for it).
//
// If the addressed object already has work in flight, the event is parked in
// the object's coalescing slot (replacing any previously parked event) and
// will run right after the in-flight work finishes; the queue does not grow.
//
// If the object is new and the queue is at capacity, Submit returns
// ErrQueueFull and the event is not accepted.
func (w *Worker) Submit(ev model.Event) error {
	w.mu.Lock()
	if ev.EventID != "" && w.seen[ev.EventID] {
		w.mu.Unlock()
		return nil
	}
	if ev.EventID != "" {
		w.seen[ev.EventID] = true
	}
	key := ev.Key()
	if w.inflight[key] {
		// Same object already has work queued/processing: park the latest
		// intent in the coalescing slot instead of stacking queue entries.
		// The slot holds at most one event, so a burst of N changes for one
		// object costs O(1) queue space; the parked event is processed after
		// the in-flight one, and because processing re-reads the newest
		// desired state, the convergence result is unchanged.
		w.pending.Add(1)
		if parked, ok := w.coalesced[key]; ok {
			if supersedes(ev, parked.ev) {
				w.coalesced[key] = queuedEvent{ev: ev, attempt: 0}
			}
			// Exactly one of (parked, new) survives; settle the loser's
			// pending count so Drain does not wait on work that will never
			// run. Add-before-Done keeps the counter from touching zero
			// transiently.
			w.pending.Done()
		} else {
			w.coalesced[key] = queuedEvent{ev: ev, attempt: 0}
		}
		w.mu.Unlock()
		return nil
	}
	if w.cfg.MaxQueueSize > 0 && len(w.queue) >= w.cfg.MaxQueueSize {
		// Backpressure: reject without marking the event seen, so the caller
		// can re-submit once the backlog drains.
		if ev.EventID != "" {
			delete(w.seen, ev.EventID)
		}
		w.mu.Unlock()
		return ErrQueueFull
	}
	w.inflight[key] = true
	w.pending.Add(1)
	w.queue = append(w.queue, queuedEvent{ev: ev, attempt: 0})
	w.mu.Unlock()

	w.signal()
	return nil
}

// supersedes reports whether a newly arrived event may replace the event
// currently parked in an object's coalescing slot.
//
//   - PUT vs PUT: the higher generation wins — the last COMMIT, not the last
//     arrival. A late-arriving older PUT must not evict a newer one, or the
//     object could lose its newest intent and strand at an intermediate value.
//   - Different event types (e.g. PUT vs DELETE): the latest arrival decides,
//     because deletes/recreates are ordered by the control plane's delivery
//     order rather than by generation.
func supersedes(new, old model.Event) bool {
	if new.Type != old.Type {
		return true
	}
	if new.Type == model.EventPut {
		return new.Generation >= old.Generation
	}
	return true
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
		res := w.rec.Process(w.ctx, item.ev)
		res.Attempt = item.attempt + 1

		if !res.Converged && res.ErrClass == model.ErrorClassTransient &&
			item.attempt < w.cfg.MaxRetries {
			// Transient failure: retain the event and retry after backoff. The
			// pending counter stays held until the retry eventually finishes,
			// so the event cannot be lost.
			item.attempt++
			w.mu.Lock()
			w.retrying++
			w.mu.Unlock()
			go w.requeueAfter(*item, w.backoff(item.attempt))
			continue
		}

		// Terminal: converged success, permanent failure, or retries exhausted.
		w.finish(item.ev.Key(), res)
	}
}

// pop blocks until an event is available. After the context is cancelled it
// still drains any events already queued, and only returns nil once the queue
// is empty, so accepted events are processed before shutdown completes.
func (w *Worker) pop() *queuedEvent {
	for {
		w.mu.Lock()
		if len(w.queue) > 0 {
			item := w.queue[0]
			w.queue = w.queue[1:]
			w.mu.Unlock()
			return &item
		}
		cancelled := w.ctx.Err() != nil
		w.mu.Unlock()
		if cancelled {
			return nil
		}

		select {
		case <-w.ctx.Done():
			// Re-enter the loop to drain whatever is still queued.
		case <-w.notify:
		}
	}
}

// requeueAfter puts an event back after the backoff delay, or aborts it (and
// any event coalesced behind it) if the worker is stopped during the wait.
func (w *Worker) requeueAfter(item queuedEvent, delay time.Duration) {
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-w.ctx.Done():
		w.abortRetry(item, "worker stopped before retry")
		return
	case <-t.C:
	}
	w.mu.Lock()
	if w.ctx.Err() != nil {
		w.mu.Unlock()
		w.abortRetry(item, "worker stopped before retry")
		return
	}
	w.retrying--
	w.queue = append(w.queue, item)
	w.mu.Unlock()
	w.signal()
}

// abortRetry terminally settles an event that can no longer be retried because
// the worker is stopping. Any event parked in the object's coalescing slot is
// settled too, so Drain can never block on work that will never run.
func (w *Worker) abortRetry(item queuedEvent, reason string) {
	key := item.ev.Key()
	w.mu.Lock()
	w.retrying--
	w.results[key] = model.ActionResult{
		DeviceID: item.ev.DeviceID, Feature: item.ev.Feature,
		Generation: item.ev.Generation, Action: model.ActionNone,
		ErrClass: model.ErrorClassTransient, Reason: reason,
		Attempt: item.attempt + 1,
	}
	settled := 1
	if next, ok := w.coalesced[key]; ok {
		delete(w.coalesced, key)
		w.results[key] = model.ActionResult{
			DeviceID: next.ev.DeviceID, Feature: next.ev.Feature,
			Generation: next.ev.Generation, Action: model.ActionNone,
			ErrClass: model.ErrorClassTransient, Reason: reason,
		}
		settled++
	}
	w.inflight[key] = false
	w.mu.Unlock()
	for i := 0; i < settled; i++ {
		w.pending.Done()
	}
}

// finish releases the object's terminal result and promotes any coalesced
// event into the queue, keeping per-object work bounded at one in-flight plus
// one waiting entry.
func (w *Worker) finish(key string, res model.ActionResult) {
	w.mu.Lock()
	w.results[key] = res
	promoted := false
	if next, ok := w.coalesced[key]; ok {
		delete(w.coalesced, key)
		w.queue = append(w.queue, next)
		promoted = true
	} else {
		w.inflight[key] = false
	}
	w.mu.Unlock()
	w.pending.Done()
	if promoted {
		w.signal()
	}
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

// Drain blocks until every accepted event has reached a terminal result,
// including events parked in retry backoff or coalescing slots.
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

// Stats returns a snapshot of the current backlog: how many events are
// queued, how many objects have work in flight, how many events are parked in
// retry backoff, and how many are waiting in coalescing slots. Use it to
// detect accumulation (e.g. a growing Queued or Retrying count).
func (w *Worker) Stats() Stats {
	w.mu.Lock()
	defer w.mu.Unlock()
	inflight := 0
	for _, v := range w.inflight {
		if v {
			inflight++
		}
	}
	return Stats{
		Queued:    len(w.queue),
		Inflight:  inflight,
		Retrying:  w.retrying,
		Coalesced: len(w.coalesced),
	}
}
