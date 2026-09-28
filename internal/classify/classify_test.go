package classify

import (
	"errors"
	"testing"

	"github.com/xingjianluo/device-config-reconciler-go/internal/model"
)

func TestClassify(t *testing.T) {
	cases := []struct {
		name    string
		trigger string // condition under test
		err     error
		want    model.ErrorClass
	}{
		{
			name:    "nil error means no failure",
			trigger: "device operation succeeded with nil error",
			err:     nil,
			want:    model.ErrorClassNone,
		},
		{
			name:    "explicit transient error",
			trigger: "device returns temporary lock/unavailable error",
			err:     NewTransient("temporarily locked"),
			want:    model.ErrorClassTransient,
		},
		{
			name:    "explicit permanent error",
			trigger: "device rejects illegal field payload",
			err:     NewPermanent("invalid mode"),
			want:    model.ErrorClassPermanent,
		},
		{
			name:    "unknown error fails closed as transient",
			trigger: "an untyped/unexpected error reaches the classifier",
			err:     errors.New("boom"),
			want:    model.ErrorClassTransient,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("trigger=%s | verdict rule: nil=>NONE, explicit PERMANENT=>PERMANENT, otherwise=>TRANSIENT", tc.trigger)
			got := Classify(tc.err)
			if got != tc.want {
				t.Fatalf("Classify(%v) = %q, want %q", tc.err, got, tc.want)
			}
			t.Logf("verdict: got class=%q matches expected %q -> PASS", got, tc.want)
		})
	}
}

func TestErrorMessagesAreDistinguishable(t *testing.T) {
	tr := NewTransient("device locked")
	pe := NewPermanent("level out of range")
	t.Logf("trigger=build one transient and one permanent error | verdict: prefixes must differ and be self-describing")
	if tr.Error() == pe.Error() {
		t.Fatalf("transient and permanent error strings must be distinguishable: %q", tr.Error())
	}
	t.Logf("transient=%q permanent=%q -> PASS", tr.Error(), pe.Error())
	if Reason(nil) != "" {
		t.Fatalf("Reason(nil) = %q, want empty string", Reason(nil))
	}
}
