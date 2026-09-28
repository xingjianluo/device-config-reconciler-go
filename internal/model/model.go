// Package model defines the domain types shared by the local device-config
// reconciler: desired configuration, observed device state, events,
// reconciliation actions, bookkeeping and externally visible results.
package model

import "time"

// ActionKind enumerates the operations that can be issued towards a device.
type ActionKind string

const (
	ActionNone    ActionKind = ""        // already converged, nothing to do
	ActionCreate  ActionKind = "CREATE"  // device has no state yet
	ActionUpdate  ActionKind = "UPDATE"  // drift detected, correct it
	ActionCleanup ActionKind = "CLEANUP" // desired deleted, clear device state
)

// EventType distinguishes desired-state change events.
type EventType string

const (
	EventPut    EventType = "PUT"    // create or update desired config
	EventDelete EventType = "DELETE" // desired config removed
	EventResync EventType = "RESYNC" // periodic / drift re-check trigger
)

// Spec is the desired configuration payload of a (device, feature).
type Spec struct {
	Enabled bool   `json:"enabled"`
	Mode    string `json:"mode"`
	Level   int    `json:"level"`
}

// DesiredConfig is the intended state for one (DeviceID, Feature) object.
type DesiredConfig struct {
	DeviceID   string
	Feature    string
	Spec       Spec
	Generation int64
	UpdatedAt  time.Time
}

// DeviceState is the simulated on-device actual state.
type DeviceState struct {
	Enabled   bool
	Mode      string
	Level     int
	UpdatedAt time.Time
}

// LedgerEntry records that one generation was successfully applied to the
// device, which is the basis of convergence bookkeeping.
type LedgerEntry struct {
	Generation int64
	AppliedAt  time.Time
	Action     ActionKind
}

// ErrorClass partitions failures into retryable and terminal categories.
type ErrorClass string

const (
	ErrorClassNone      ErrorClass = ""
	ErrorClassTransient ErrorClass = "TRANSIENT" // backoff & retry, event retained
	ErrorClassPermanent ErrorClass = "PERMANENT" // stop retrying, reason recorded
)

// ActionResult is the externally visible outcome of processing an object once.
type ActionResult struct {
	DeviceID   string
	Feature    string
	Generation int64
	Action     ActionKind
	Converged  bool
	ErrClass   ErrorClass
	Reason     string
	Attempt    int
}

// Event is a unit of work delivered to the reconciler.
type Event struct {
	// EventID enables idempotent processing: re-delivered events carrying the
	// same ID must produce exactly the same outcome as the first delivery.
	EventID  string
	Type     EventType
	DeviceID string
	Feature  string
	// Spec is populated for PUT events; ignored otherwise.
	Spec *Spec
	// Generation is carried by PUT events and is stored with the desired config.
	Generation int64
}

// Key returns the object identity addressed by an event.
func (e Event) Key() string {
	return e.DeviceID + "/" + e.Feature
}
