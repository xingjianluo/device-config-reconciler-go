package reconciler

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/xingjianluo/device-config-reconciler-go/internal/classify"
	"github.com/xingjianluo/device-config-reconciler-go/internal/model"
	"github.com/xingjianluo/device-config-reconciler-go/internal/storage"
)

// DeviceClient is the subset of the device surface used by the reconciler. The
// production implementation is *device.Client; tests may wrap it to count or
// block device calls.
type DeviceClient interface {
	Get(deviceID, feature string) (model.DeviceState, bool)
	Apply(deviceID, feature string, spec model.Spec, gen int64) error
	Cleanup(deviceID, feature string) error
}

// Reconciler processes one object (device/feature) at a time under a per-object
// lock. Concurrent Process calls for the same object are serialized, so the
// device cannot receive duplicate operations and generations cannot be
// double-recorded; different objects proceed in parallel.
//
// Generational gating: every intent (PUT/DELETE) carrying a positive Generation
// is ordered per object. An event whose generation is older than an intent
// already accepted is acknowledged as Stale without touching the device, so a
// late/duplicated old change can never roll state backwards. tombstones
// remember the generation of the latest completed DELETE so that an old PUT
// arriving after that DELETE cannot resurrect a deleted object; a newer PUT
// clears the tombstone again (delete/recreate interleaving).
type Reconciler struct {
	store      storage.Store
	client     DeviceClient
	locksMu    sync.Mutex
	locks      map[string]*sync.Mutex
	tombstones map[string]int64
}

// New builds a Reconciler over the given store and device client.
func New(store storage.Store, client DeviceClient) *Reconciler {
	return &Reconciler{
		store:      store,
		client:     client,
		locks:      make(map[string]*sync.Mutex),
		tombstones: make(map[string]int64),
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
	key := ev.DeviceID + "/" + ev.Feature

	// Generational ordering against a newer intent already accepted.
	if ev.Generation > 0 {
		if newer, ok := r.store.GetDesired(ev.DeviceID, ev.Feature); ok && newer.Generation > ev.Generation {
			return r.stale(ev, "stale PUT gen "+strconv.FormatInt(ev.Generation, 10)+
				", newer desired gen "+strconv.FormatInt(newer.Generation, 10)+" retained")
		}
		r.locksMu.Lock()
		tombGen := r.tombstones[key]
		r.locksMu.Unlock()
		if tombGen >= ev.Generation {
			return r.stale(ev, "stale PUT gen "+strconv.FormatInt(ev.Generation, 10)+
				", object deleted at gen "+strconv.FormatInt(tombGen, 10))
		}
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
	// A fresh intent supersedes any prior deletion: the tombstone must not
	// reject the recreate.
	if ev.Generation > 0 {
		r.locksMu.Lock()
		delete(r.tombstones, key)
		r.locksMu.Unlock()
	}
	return r.converge(ev.DeviceID, ev.Feature, ev.Generation, 0)
}

func (r *Reconciler) handleDelete(ev model.Event) model.ActionResult {
	key := ev.DeviceID + "/" + ev.Feature

	// A versioned DELETE older than a still-present desired intent is stale: it
	// must neither remove the desired config nor touch the device.
	if ev.Generation > 0 {
		if newer, ok := r.store.GetDesired(ev.DeviceID, ev.Feature); ok && newer.Generation > ev.Generation {
			return r.stale(ev, "stale DELETE gen "+strconv.FormatInt(ev.Generation, 10)+
				", newer desired gen "+strconv.FormatInt(newer.Generation, 10)+" retained")
		}
	}

	// Intent to delete is recorded up front; repeated DELETE events simply
	// re-attempt the cleanup until it is confirmed done.
	r.store.DeleteDesired(ev.DeviceID, ev.Feature)
	if ev.Generation > 0 {
		r.locksMu.Lock()
		if ev.Generation > r.tombstones[key] {
			r.tombstones[key] = ev.Generation
		}
		r.locksMu.Unlock()
	}

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

// stale acknowledges an out-of-order old event without changing any state. It
// is intentionally NOT passed to store.RecordResult: an object's latest
// externally visible result must belong to its newest intent.
func (r *Reconciler) stale(ev model.Event, reason string) model.ActionResult {
	return model.ActionResult{
		DeviceID:   ev.DeviceID,
		Feature:    ev.Feature,
		Generation: ev.Generation,
		Action:     model.ActionNone,
		Converged:  true,
		Reason:     reason,
		Stale:      true,
	}
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
