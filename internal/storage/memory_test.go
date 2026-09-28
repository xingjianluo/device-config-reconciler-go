package storage

import (
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/device-config-reconciler-go/internal/model"
)

func TestDesiredCRUD(t *testing.T) {
	s := NewMemoryStore()
	t.Log("trigger=put a desired config then read it | verdict: round trip must preserve fields")
	cfg := model.DesiredConfig{
		DeviceID: "dev-1", Feature: "wifi",
		Spec:       model.Spec{Enabled: true, Mode: "auto", Level: 5},
		Generation: 1, UpdatedAt: time.Now(),
	}
	s.PutDesired(cfg)
	got, ok := s.GetDesired("dev-1", "wifi")
	if !ok || got.Generation != 1 || got.Spec.Level != 5 {
		t.Fatalf("GetDesired returned %+v ok=%v, want stored config gen=1 level=5", got, ok)
	}
	t.Logf("verdict: got gen=%d level=%d -> PASS", got.Generation, got.Spec.Level)

	t.Log("trigger=overwrite with generation 2 | verdict: update must replace old value")
	cfg.Generation, cfg.Spec.Level = 2, 6
	s.PutDesired(cfg)
	got, _ = s.GetDesired("dev-1", "wifi")
	if got.Generation != 2 || got.Spec.Level != 6 {
		t.Fatalf("after update got gen=%d level=%d, want 2/6", got.Generation, got.Spec.Level)
	}
	t.Log("verdict: update visible -> PASS")

	t.Log("trigger=delete desired twice | verdict: first delete true, second false")
	if !s.DeleteDesired("dev-1", "wifi") {
		t.Fatal("first DeleteDesired should return true")
	}
	if s.DeleteDesired("dev-1", "wifi") {
		t.Fatal("second DeleteDesired should return false")
	}
	t.Log("verdict: delete existence flags correct -> PASS")
}

func TestLedgerAppendAndClear(t *testing.T) {
	s := NewMemoryStore()
	t.Log("trigger=record generations 1,2,3 | verdict: ledger keeps append order, LastLedger=3")
	for i := int64(1); i <= 3; i++ {
		s.RecordLedger("dev", "f", model.LedgerEntry{Generation: i, AppliedAt: time.Now()})
	}
	if l := s.Ledger("dev", "f"); len(l) != 3 || l[0].Generation != 1 || l[2].Generation != 3 {
		t.Fatalf("ledger = %+v, want [1 2 3]", l)
	}
	last, ok := s.LastLedger("dev", "f")
	if !ok || last.Generation != 3 {
		t.Fatalf("LastLedger = %d ok=%v, want 3/true", last.Generation, ok)
	}
	t.Log("verdict: ledger order and last record correct -> PASS")

	t.Log("trigger=mutate returned ledger slice | verdict: internal state stays isolated")
	l := s.Ledger("dev", "f")
	l[0].Generation = 99
	if s.Ledger("dev", "f")[0].Generation != 1 {
		t.Fatal("returned slice must be a copy, internal ledger was mutated")
	}
	t.Log("verdict: defensive copy holds -> PASS")

	s.ClearLedger("dev", "f")
	if l := s.Ledger("dev", "f"); len(l) != 0 {
		t.Fatalf("after clear ledger len=%d, want 0", len(l))
	}
}

func TestConcurrentAccessIsSafe(t *testing.T) {
	s := NewMemoryStore()
	t.Log("trigger=50 goroutines x 100 rounds mixing put/record/get | verdict: no data race (run under -race), state stays consistent")
	var wg sync.WaitGroup
	for g := 0; g < 50; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				s.PutDesired(model.DesiredConfig{DeviceID: "d", Feature: "f", Generation: int64(i)})
				s.RecordLedger("d", "f", model.LedgerEntry{Generation: int64(i)})
				_, _ = s.GetDesired("d", "f")
				_ = s.Ledger("d", "f")
				s.RecordResult(model.ActionResult{DeviceID: "d", Feature: "f", Generation: int64(i)})
				_, _ = s.GetResult("d", "f")
			}
		}()
	}
	wg.Wait()
	// No race under -race is the primary assertion; also verify final shape.
	if n := len(s.Ledger("d", "f")); n != 50*100 {
		t.Fatalf("ledger len=%d, want %d (every append must be retained)", n, 50*100)
	}
	t.Logf("verdict: %d ledger entries retained, race detector silent -> PASS", 50*100)
}
