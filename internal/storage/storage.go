// Package storage defines persistence for desired configs and convergence
// bookkeeping. The implementation is in-memory and concurrency-safe so it can
// stand in for a real local store during tests.
package storage

import "github.com/xingjianluo/device-config-reconciler-go/internal/model"

// Store persists desired configs and per-object generation ledgers.
// Implementations must be safe for concurrent use.
type Store interface {
	// PutDesired stores (create or update) a desired config.
	PutDesired(cfg model.DesiredConfig)
	// GetDesired returns the desired config and whether it exists.
	GetDesired(deviceID, feature string) (model.DesiredConfig, bool)
	// DeleteDesired removes a desired config. It returns whether it existed.
	DeleteDesired(deviceID, feature string) bool

	// Ledger returns the recorded generations for an object, newest last.
	Ledger(deviceID, feature string) []model.LedgerEntry
	// RecordLedger appends a generation record for an object.
	RecordLedger(deviceID, feature string, entry model.LedgerEntry)
	// LastLedger returns the newest ledger entry and whether one exists.
	LastLedger(deviceID, feature string) (model.LedgerEntry, bool)
	// ClearLedger removes all ledger records for an object.
	ClearLedger(deviceID, feature string)

	// RecordResult keeps the latest externally visible result per object.
	RecordResult(r model.ActionResult)
	// GetResult returns the latest recorded result for an object.
	GetResult(deviceID, feature string) (model.ActionResult, bool)
}
