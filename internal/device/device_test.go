package device

import (
	"testing"

	"github.com/xingjianluo/device-config-reconciler-go/internal/classify"
	"github.com/xingjianluo/device-config-reconciler-go/internal/model"
)

func TestApplyAndCleanup(t *testing.T) {
	c := NewClient()
	t.Log("trigger=Apply on absent object | verdict: state created, Get finds it")
	if err := c.Apply("dev", "wifi", model.Spec{Enabled: true, Mode: "auto", Level: 3}, 1); err != nil {
		t.Fatalf("Apply returned unexpected error: %v", err)
	}
	st, ok := c.Get("dev", "wifi")
	if !ok || !st.Enabled || st.Mode != "auto" || st.Level != 3 {
		t.Fatalf("device state = %+v ok=%v", st, ok)
	}
	t.Log("verdict: state present and matches payload -> PASS")

	t.Log("trigger=Cleanup | verdict: state gone, Get returns false")
	if err := c.Cleanup("dev", "wifi"); err != nil {
		t.Fatalf("Cleanup error: %v", err)
	}
	if _, ok := c.Get("dev", "wifi"); ok {
		t.Fatal("state must be removed after cleanup")
	}
	t.Log("verdict: cleanup confirmed -> PASS")
}

func TestTransientFailureCountdown(t *testing.T) {
	c := NewClient()
	c.InjectTransientFailure("dev", "f", 2)
	t.Log("trigger=inject 2 transient failures then Apply 3 times | verdict: first two TRANSIENT, third succeeds")
	for i := 1; i <= 2; i++ {
		err := c.Apply("dev", "f", model.Spec{Mode: "auto"}, int64(i))
		if classify.Classify(err) != model.ErrorClassTransient {
			t.Fatalf("attempt %d: class=%v, want TRANSIENT (err=%v)", i, classify.Classify(err), err)
		}
		t.Logf("attempt %d: got TRANSIENT as expected", i)
	}
	if err := c.Apply("dev", "f", model.Spec{Mode: "auto"}, 3); err != nil {
		t.Fatalf("attempt 3 should succeed, got %v", err)
	}
	if _, ok := c.Get("dev", "f"); !ok {
		t.Fatal("after countdown, state must be written")
	}
	t.Log("verdict: failures consumed then operation succeeds -> PASS")
}

func TestPermanentFailure(t *testing.T) {
	c := NewClient()
	c.InjectPermanentFailure("dev", "f")
	t.Log("trigger=inject permanent failure, Apply twice | verdict: every call PERMANENT, no state written")
	for i := 1; i <= 2; i++ {
		err := c.Apply("dev", "f", model.Spec{Mode: "auto"}, int64(i))
		if classify.Classify(err) != model.ErrorClassPermanent {
			t.Fatalf("attempt %d: class=%v, want PERMANENT", i, classify.Classify(err))
		}
	}
	if _, ok := c.Get("dev", "f"); ok {
		t.Fatal("permanent failure must not produce device state")
	}
	t.Log("verdict: consistently PERMANENT and nothing persisted -> PASS")

	t.Log("trigger=Cleanup under permanent injection | verdict: also PERMANENT (delete cannot finish)")
	if got := classify.Classify(c.Cleanup("dev", "f")); got != model.ErrorClassPermanent {
		t.Fatalf("Cleanup class=%v, want PERMANENT", got)
	}
	t.Log("verdict: cleanup permanent failure visible -> PASS")
}
