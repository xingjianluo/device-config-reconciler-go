package reconciler

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/xingjianluo/device-config-reconciler-go/internal/device"
	"github.com/xingjianluo/device-config-reconciler-go/internal/model"
	"github.com/xingjianluo/device-config-reconciler-go/internal/storage"
)

// countingClient wraps a device client and counts actual Apply/Cleanup calls,
// letting the test assert that concurrent processing never double-issues.
type countingClient struct {
	*device.Client
	mu      sync.Mutex
	applies int
}

// TestConcurrentSameObjectNoDuplicateIssue fans N goroutines at the same
// object. Exactly one of them may perform the CREATE; every other must observe
// convergence and no-op.
func TestConcurrentSameObjectNoDuplicateIssue(t *testing.T) {
	store, client := storage.NewMemoryStore(), device.NewClient()
	rec := New(store, client)
	ctx := context.Background()

	const n = 64
	t.Logf("trigger=%d goroutines Process the same gen1 PUT concurrently | verdict: exactly 1 CREATE-equivalent state, ledger len 1, all results converged", n)

	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make([]model.ActionResult, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ev := putEvent(fmt.Sprintf("e-%d", i), "dev", "shared", 1, spec(true, "auto", 7))
			<-start
			results[i] = rec.Process(ctx, ev)
		}(i)
	}
	close(start)
	wg.Wait()

	createCount, convergedCount := 0, 0
	for _, r := range results {
		if r.Action == model.ActionCreate {
			createCount++
		}
		if r.Converged {
			convergedCount++
		}
	}
	if createCount != 1 {
		t.Fatalf("CREATE issued %d times, want exactly 1 (no duplicate dispatch)", createCount)
	}
	if convergedCount != n {
		t.Fatalf("converged results=%d, want %d", convergedCount, n)
	}
	if n := rec.LedgerLength("dev", "shared"); n != 1 {
		t.Fatalf("ledger len=%d, want 1 (no double bookkeeping)", n)
	}
	st, ok := client.Get("dev", "shared")
	if !ok || st.Level != 7 {
		t.Fatalf("device state=%+v ok=%v, want single convergent state level 7", st, ok)
	}
	t.Logf("verdict: CREATE=%d, converged=%d/%d, ledger=1, single device state -> PASS",
		createCount, convergedCount, n)
}

// TestConcurrentDifferentObjectsAreIndependent checks that many objects
// converge in parallel without cross-talk.
func TestConcurrentDifferentObjectsAreIndependent(t *testing.T) {
	store, client := storage.NewMemoryStore(), device.NewClient()
	rec := New(store, client)
	ctx := context.Background()

	const objs = 50
	t.Logf("trigger=%d different objects processed concurrently | verdict: each converges to its own level", objs)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < objs; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			dev := fmt.Sprintf("dev-%d", i)
			ev := putEvent("e-"+dev, dev, "wifi", 1, spec(true, "auto", i))
			<-start
			if r := rec.Process(ctx, ev); !r.Converged || r.Action != model.ActionCreate {
				t.Errorf("object %s result=%+v, want converged CREATE", dev, r)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	for i := 0; i < objs; i++ {
		dev := fmt.Sprintf("dev-%d", i)
		st, ok := client.Get(dev, "wifi")
		if !ok || st.Level != i {
			t.Fatalf("object %s state=%+v ok=%v, want level %d", dev, st, ok, i)
		}
	}
	t.Logf("verdict: all %d independent objects converged correctly -> PASS", objs)
}
