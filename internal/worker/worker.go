package worker

import (
	"context"
	"sync"
	"time"

	"github.com/xingjianluo/device-config-reconciler-go/internal/model"
	"github.com/xingjianluo/device-config-reconciler-go/internal/reconciler"
)

// queuedEvent pairs an event with its current attempt counter.
type queuedEvent struct {
	ev      model.Event
	attempt int
}

// Worker drives reconciliation from an in-memory event queue.
type Worker struct {
	rec *reconciler.Reconciler
	cfg Config

	mu       sync.Mutex
	seen     map[string]bool // event IDs ever accepted -> duplicate suppression
	inflight map[string]bool // object keys currently queued/processing
	queue    []queuedEvent
	results  map[string]model.ActionResult
	notify   chan struct{}

	// pending counts events that have been accepted but not yet finished
	// (including events parked in a retry backoff). Drain waits on it.
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
	return &Worker{
		rec:      rec,
		cfg:      cfg,
		seen:     make(map[string]bool),
		inflight: make(map[string]bool),
		results:  make(map[string]model.ActionResult),
		notify:   make(chan struct{}, 1),
	}
}

// Submit enqueues an event. An event whose EventID was already submitted is a
// duplicate delivery and is coalesced: it is processed at most once, so its
// effect is identical to a single delivery.
func (w *Worker) Submit(ev model.Event) {
	w.mu.Lock()
	if ev.EventID != "" && w.seen[ev.EventID] {
		w.mu.Unlock()
		return
	}
	if ev.EventID != "" {
		w.seen[ev.EventID] = true
	}
	// If the same object already has an event queued/in-flight, do not stack a
	// second one: the in-flight processing (re)reads the stored desired state
	// and is itself idempotent, so a stacked entry would only be redundant
	// work. This prevents duplicate device operations under event bursts.
	if w.inflight[ev.Key()] {
		w.mu.Unlock()
		return
	}
	w.inflight[ev.Key()] = true
	w.pending.Add(1)
	w.queue = append(w.queue, queuedEvent{ev: ev, attempt: 0})
	w.mu.Unlock()

	w.signal()
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

// requeueAfter puts an event back after the backoff delay, or finishes it as a
// transient failure if the worker is stopped during the wait.
func (w *Worker) requeueAfter(item queuedEvent, delay time.Duration) {
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-w.ctx.Done():
		w.finish(item.ev.Key(), model.ActionResult{
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
	if w.ctx.Err() != nil {
		w.mu.Unlock()
		w.finish(item.ev.Key(), model.ActionResult{
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

// finish releases the object's in-flight slot, stores the external result and
// marks the event complete.
func (w *Worker) finish(key string, res model.ActionResult) {
	w.mu.Lock()
	w.inflight[key] = false
	w.results[key] = res
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

// Drain blocks until every accepted event has reached a terminal result,
// including events parked in retry backoff.
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
