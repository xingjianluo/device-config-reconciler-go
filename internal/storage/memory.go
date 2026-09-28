package storage

import (
	"sync"

	"github.com/xingjianluo/device-config-reconciler-go/internal/model"
)

// MemoryStore is an in-memory, concurrency-safe Store implementation. It stands
// in for the local persistence layer (e.g. a bolt/sqlite file) in tests.
type MemoryStore struct {
	mu      sync.Mutex
	desired map[string]model.DesiredConfig
	ledgers map[string][]model.LedgerEntry
	results map[string]model.ActionResult
}

// NewMemoryStore creates an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		desired: make(map[string]model.DesiredConfig),
		ledgers: make(map[string][]model.LedgerEntry),
		results: make(map[string]model.ActionResult),
	}
}

func key(deviceID, feature string) string { return deviceID + "/" + feature }

func (s *MemoryStore) PutDesired(cfg model.DesiredConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.desired[key(cfg.DeviceID, cfg.Feature)] = cfg
}

func (s *MemoryStore) GetDesired(deviceID, feature string) (model.DesiredConfig, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg, ok := s.desired[key(deviceID, feature)]
	return cfg, ok
}

func (s *MemoryStore) DeleteDesired(deviceID, feature string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(deviceID, feature)
	if _, ok := s.desired[k]; !ok {
		return false
	}
	delete(s.desired, k)
	return true
}

func (s *MemoryStore) Ledger(deviceID, feature string) []model.LedgerEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	src := s.ledgers[key(deviceID, feature)]
	if len(src) == 0 {
		return nil
	}
	// Return a copy so callers cannot mutate stored records.
	out := make([]model.LedgerEntry, len(src))
	copy(out, src)
	return out
}

func (s *MemoryStore) RecordLedger(deviceID, feature string, entry model.LedgerEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(deviceID, feature)
	s.ledgers[k] = append(s.ledgers[k], entry)
}

func (s *MemoryStore) LastLedger(deviceID, feature string) (model.LedgerEntry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := s.ledgers[key(deviceID, feature)]
	if len(entries) == 0 {
		return model.LedgerEntry{}, false
	}
	return entries[len(entries)-1], true
}

func (s *MemoryStore) ClearLedger(deviceID, feature string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.ledgers, key(deviceID, feature))
}

func (s *MemoryStore) RecordResult(r model.ActionResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results[key(r.DeviceID, r.Feature)] = r
}

func (s *MemoryStore) GetResult(deviceID, feature string) (model.ActionResult, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.results[key(deviceID, feature)]
	return r, ok
}
