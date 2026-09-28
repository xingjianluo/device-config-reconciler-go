// Package classify maps low-level errors to the reconciler's transient vs
// permanent failure taxonomy.
package classify

import "github.com/xingjianluo/device-config-reconciler-go/internal/model"

// Error is an error carrying an explicit failure class.
type Error struct {
	Class  model.ErrorClass
	Reason string
}

func (e *Error) Error() string {
	return string(e.Class) + ": " + e.Reason
}

// NewTransient builds a transient (retryable) error: temporary lock contention,
// brief unavailability, timeouts, etc. The event must be retained and retried
// with backoff.
func NewTransient(reason string) *Error {
	return &Error{Class: model.ErrorClassTransient, Reason: reason}
}

// NewPermanent builds a permanent (non-retryable) error: illegal fields, out of
// range parameters, etc. Retrying cannot help, so processing stops and the
// reason is recorded on the external result.
func NewPermanent(reason string) *Error {
	return &Error{Class: model.ErrorClassPermanent, Reason: reason}
}

// Classify returns the class of err.
//
//   - *Error with an explicit class wins;
//   - a nil error means no failure;
//   - unknown/untyped errors default to TRANSIENT so an unexpected problem
//     never silently drops an event (fail closed).
func Classify(err error) model.ErrorClass {
	if err == nil {
		return model.ErrorClassNone
	}
	if e, ok := err.(*Error); ok {
		if e.Class == model.ErrorClassPermanent {
			return model.ErrorClassPermanent
		}
		return model.ErrorClassTransient
	}
	return model.ErrorClassTransient
}

// Reason extracts a human-readable reason from err, or "" for nil.
func Reason(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
