package reconciler

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/xingjianluo/device-config-reconciler-go/internal/classify"
	"github.com/xingjianluo/device-config-reconciler-go/internal/device"
	"github.com/xingjianluo/device-config-reconciler-go/internal/model"
	"github.com/xingjianluo/device-config-reconciler-go/internal/storage"
)

// Reconciler processes one object (device/feature) at a time under a per-object
// lock. Concurrent Process calls for the same object are serialized, so the
// device cannot receive duplicate operations and generations cannot be
// double-recorded; different objects proceed in parallel.
type Reconciler struct {
	store  storage.Store
	client *device.Client

	locksMu sync.Mutex
	locks   map[string]*sync.Mutex
}

// New builds a Reconciler over the given store and device client.
func New(store storage.Store, client *device.Client) *Reconciler {
	return &Reconciler{
		store:  store,
		client: client,
		locks:  make(map[string]*sync.Mutex),
	}
}

// lockFor returns the dedicated mutex for one object.
func (r *Reconciler) lockFor(deviceID, feature string) *sync.Mutex {
	r.locksMu.Lock()
	defer r.locksMu.Unlock()
	key := deviceID + "/" + feature
	m, ok := r.locks[key]
	if !ok {
		m = &sync.Mutex{}
		r.locks[key] = m
	}
	return m
}

// Process handles one event and records/returns its externally visible result.
//
// Semantics:
//
//   - PUT: persist the desired config (create/update), then converge the device
//     onto it. Idempotent: if the target generation is already the last
//     recorded generation and the device still matches, no operation is issued.
//   - DELETE: deletion is finished only after the device cleanup succeeds. The
//     desired config is removed up front; a still-present device state (or a
//     transient cleanup failure) keeps the object non-converged until cleanup
//     completes, at which point device state and ledger are cleared.
//   - RESYNC: re-read desired vs actual and correct any drift.
func (r *Reconciler) Process(ctx context.Context, ev model.Event) model.ActionResult {
	lock := r.lockFor(ev.DeviceID, ev.Feature)
	lock.Lock()
	defer lock.Unlock()

	switch ev.Type {
	case model.EventPut:
		return r.handlePut(ev)
	case model.EventDelete:
		return r.handleDelete(ev)
	case model.EventResync:
		return r.handleResync(ev)
	default:
		res := model.ActionResult{
			DeviceID: ev.DeviceID, Feature: ev.Feature,
			ErrClass: model.ErrorClassPermanent,
			Reason:   "unknown event type: " + string(ev.Type),
		}
		r.store.RecordResult(res)
		return res
	}
}

func (r *Reconciler) handlePut(ev model.Event) model.ActionResult {
	if ev.Spec == nil {
		res := r.fail(ev, model.ActionNone, classify.NewPermanent("PUT event missing spec"))
		return res
	}
	// Out-of-order guard: a late-arriving PUT whose generation is older than
	// the stored desired generation must never drag the object backwards. It
	// is not silently dropped either: convergence is re-driven towards the
	// newest known generation, so the object cannot strand at an intermediate
	// value even if the newest event's own delivery is still being retried.
	if existing, ok := r.store.GetDesired(ev.DeviceID, ev.Feature); ok && ev.Generation < existing.Generation {
		res := r.converge(ev.DeviceID, ev.Feature, existing.Generation, 0)
		if res.Converged {
			res.Reason = "stale PUT gen=" + strconv.FormatInt(ev.Generation, 10) +
				" superseded by gen=" + strconv.FormatInt(existing.Generation, 10)
			r.store.RecordResult(res)
		}
		return res
	}
	// Persist intent before acting: a crash/re-delivery repeats the converge
	// step rather than losing the desired change.
	r.store.PutDesired(model.DesiredConfig{
		DeviceID:   ev.DeviceID,
		Feature:    ev.Feature,
		Spec:       *ev.Spec,
		Generation: ev.Generation,
		UpdatedAt:  time.Now(),
	})
	return r.converge(ev.DeviceID, ev.Feature, ev.Generation, 0)
}

func (r *Reconciler) handleDelete(ev model.Event) model.ActionResult {
	// Intent to delete is recorded up front; repeated DELETE events simply
	// re-attempt the cleanup until it is confirmed done.
	r.store.DeleteDesired(ev.DeviceID, ev.Feature)

	state, exists := r.client.Get(ev.DeviceID, ev.Feature)
	_ = state
	if !exists {
		// Cleanup already complete: the deletion is now truly finished.
		r.store.ClearLedger(ev.DeviceID, ev.Feature)
		res := model.ActionResult{
			DeviceID:  ev.DeviceID,
			Feature:   ev.Feature,
			Action:    model.ActionCleanup,
			Converged: true,
		}
		r.store.RecordResult(res)
		return res
	}

	if err := r.client.Cleanup(ev.DeviceID, ev.Feature); err != nil {
		return r.failWithErr(ev, model.ActionCleanup, err)
	}

	// Cleanup confirmed: only now clear the bookkeeping and report completion.
	r.store.ClearLedger(ev.DeviceID, ev.Feature)
	res := model.ActionResult{
		DeviceID: ev.DeviceID, Feature: ev.Feature,
		Action: model.ActionCleanup, Converged: true,
	}
	r.store.RecordResult(res)
	return res
}

func (r *Reconciler) handleResync(ev model.Event) model.ActionResult {
	cfg, ok := r.store.GetDesired(ev.DeviceID, ev.Feature)
	if !ok {
		// Nothing desired: make sure no stale device state lingers.
		return r.handleDelete(ev)
	}
	return r.converge(ev.DeviceID, ev.Feature, cfg.Generation, 0)
}

// converge drives the device toward the stored desired state. Must be called
// with the object lock held.
func (r *Reconciler) converge(deviceID, feature string, gen int64, attempt int) model.ActionResult {
	ev := model.Event{DeviceID: deviceID, Feature: feature, Generation: gen}
	cfg, ok := r.store.GetDesired(deviceID, feature)
	if !ok {
		// Desired disappeared while we worked: switch to cleanup semantics.
		return r.handleDelete(ev)
	}

	state, exists := r.client.Get(deviceID, feature)

	// Idempotency / convergence check: last ledger equals target generation and
	// the device still reflects the desired spec -> nothing to do.
	if exists && state.Enabled == cfg.Spec.Enabled &&
		state.Mode == cfg.Spec.Mode && state.Level == cfg.Spec.Level {
		if last, has := r.store.LastLedger(deviceID, feature); has && last.Generation == gen {
			res := model.ActionResult{
				DeviceID: deviceID, Feature: feature, Generation: gen,
				Action: model.ActionNone, Converged: true, Attempt: attempt,
			}
			r.store.RecordResult(res)
			return res
		}
	}

	// Permanent validation: illegal fields must never reach the device and must
	// not be retried.
	if err := validate(cfg.Spec); err != nil {
		return r.failWithErr(ev, actionFor(exists), err)
	}

	action := actionFor(exists)
	if err := r.client.Apply(deviceID, feature, cfg.Spec, gen); err != nil {
		return r.failWithErr(ev, action, err)
	}

	// Success: record the generation exactly once (we hold the object lock).
	r.store.RecordLedger(deviceID, feature, model.LedgerEntry{
		Generation: gen,
		AppliedAt:  time.Now(),
		Action:     action,
	})
	res := model.ActionResult{
		DeviceID: deviceID, Feature: feature, Generation: gen,
		Action: action, Converged: true, Attempt: attempt,
	}
	r.store.RecordResult(res)
	return res
}

// Resync forces a drift re-check for one object and corrects it.
func (r *Reconciler) Resync(ctx context.Context, deviceID, feature string) model.ActionResult {
	return r.Process(ctx, model.Event{
		Type: model.EventResync, DeviceID: deviceID, Feature: feature,
	})
}

// LedgerLength is a test/inspection helper.
func (r *Reconciler) LedgerLength(deviceID, feature string) int {
	return len(r.store.Ledger(deviceID, feature))
}

func actionFor(exists bool) model.ActionKind {
	if exists {
		return model.ActionUpdate
	}
	return model.ActionCreate
}

func validate(spec model.Spec) error {
	switch spec.Mode {
	case "auto", "manual":
	default:
		return classify.NewPermanent("invalid mode: " + spec.Mode)
	}
	if spec.Level < 0 || spec.Level > 100 {
		return classify.NewPermanent("level out of range [0,100]: " +
			strconv.Itoa(spec.Level))
	}
	return nil
}

func (r *Reconciler) fail(ev model.Event, action model.ActionKind, err *classify.Error) model.ActionResult {
	return r.failWithErr(ev, action, err)
}

func (r *Reconciler) failWithErr(ev model.Event, action model.ActionKind, err error) model.ActionResult {
	res := model.ActionResult{
		DeviceID:   ev.DeviceID,
		Feature:    ev.Feature,
		Generation: ev.Generation,
		Action:     action,
		Converged:  false,
		ErrClass:   classify.Classify(err),
		Reason:     classify.Reason(err),
	}
	r.store.RecordResult(res)
	return res
}
