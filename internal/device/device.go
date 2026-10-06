package device

import (
	"sync"
	"time"

	"github.com/xingjianluo/device-config-reconciler-go/internal/classify"
	"github.com/xingjianluo/device-config-reconciler-go/internal/model"
)

// Client simulates the device side with a local, concurrency-safe key/value
// store. Failure injection lets tests drive transient and permanent paths:
//
//   - transientFailures[key] is a countdown: each Apply/Cleanup consumes one
//     and returns a temporary "device locked / unavailable" error until it
//     reaches zero, after which operations succeed (models a short-lived lock);
//   - permanentFailures[key] makes every operation fail with a permanent error
//     (models the device rejecting the payload).
type Client struct {
	mu                sync.Mutex
	states            map[string]model.DeviceState
	transientFailures map[string]int
	permanentFailures map[string]bool
	// applyHook / cleanupHook, when set, run at the start of Apply/Cleanup
	// while the client lock is held. Tests use them to block an in-flight
	// device call and deterministically interleave freshly arriving events.
	applyHook   func(deviceID, feature string)
	cleanupHook func(deviceID, feature string)
}

// NewClient creates an empty simulated device client.
func NewClient() *Client {
	return &Client{
		states:            make(map[string]model.DeviceState),
		transientFailures: make(map[string]int),
		permanentFailures: make(map[string]bool),
	}
}

func k(deviceID, feature string) string { return deviceID + "/" + feature }

// Get returns the actual on-device state and whether it exists.
func (c *Client) Get(deviceID, feature string) (model.DeviceState, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.states[k(deviceID, feature)]
	return st, ok
}

// Apply writes spec to the device and returns an error whose class conveys
// whether the failure is transient or permanent.
func (c *Client) Apply(deviceID, feature string, spec model.Spec, gen int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := k(deviceID, feature)
	if c.applyHook != nil {
		c.applyHook(deviceID, feature)
	}
	if err := c.failure(key); err != nil {
		return err
	}
	c.states[key] = model.DeviceState{
		Enabled:   spec.Enabled,
		Mode:      spec.Mode,
		Level:     spec.Level,
		UpdatedAt: time.Now(),
	}
	return nil
}

// Cleanup removes feature state from the device.
func (c *Client) Cleanup(deviceID, feature string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := k(deviceID, feature)
	if c.cleanupHook != nil {
		c.cleanupHook(deviceID, feature)
	}
	if err := c.failure(key); err != nil {
		return err
	}
	delete(c.states, key)
	return nil
}

// failure must be called with c.mu held.
func (c *Client) failure(key string) error {
	if c.permanentFailures[key] {
		return classify.NewPermanent("device rejected payload for " + key)
	}
	if c.transientFailures[key] > 0 {
		c.transientFailures[key]--
		return classify.NewTransient("device temporarily locked for " + key)
	}
	return nil
}

// InjectTransientFailure arranges the next count Apply/Cleanup calls for the
// given object to fail transiently before succeeding.
func (c *Client) InjectTransientFailure(deviceID, feature string, count int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.transientFailures[k(deviceID, feature)] = count
}

// InjectPermanentFailure arranges all future Apply/Cleanup calls for the given
// object to fail permanently.
func (c *Client) InjectPermanentFailure(deviceID, feature string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.permanentFailures[k(deviceID, feature)] = true
}

// Snapshot returns a copy of all current device states, keyed object key. It is
// a test helper for asserting final convergence.
func (c *Client) Snapshot() map[string]model.DeviceState {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]model.DeviceState, len(c.states))
	for key, st := range c.states {
		out[key] = st
	}
	return out
}

// ResetFailures removes all injected transient/permanent failures for one
// object while leaving its current state intact. It models device recovery in
// tests (e.g. a lock being released or a service restarting).
func (c *Client) ResetFailures(deviceID, feature string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := k(deviceID, feature)
	delete(c.transientFailures, key)
	delete(c.permanentFailures, key)
}

// SetApplyHook installs a test hook invoked at the start of every Apply while
// the client lock is held; pass nil to clear it.
func (c *Client) SetApplyHook(h func(deviceID, feature string)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.applyHook = h
}

// SetCleanupHook installs a test hook invoked at the start of every Cleanup
// while the client lock is held; pass nil to clear it.
func (c *Client) SetCleanupHook(h func(deviceID, feature string)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cleanupHook = h
}
