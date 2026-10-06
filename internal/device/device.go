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
	applyCounts       map[string]int
	cleanupCounts     map[string]int
}

// NewClient creates an empty simulated device client.
func NewClient() *Client {
	return &Client{
		states:            make(map[string]model.DeviceState),
		transientFailures: make(map[string]int),
		permanentFailures: make(map[string]bool),
		applyCounts:       make(map[string]int),
		cleanupCounts:     make(map[string]int),
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
	c.applyCounts[key]++
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
	c.cleanupCounts[key]++
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

// ApplyCount reports how many Apply calls (successful or not) were issued
// towards an object. It is a test helper for asserting that coalescing and
// idempotency keep the number of device operations bounded.
func (c *Client) ApplyCount(deviceID, feature string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.applyCounts[k(deviceID, feature)]
}

// CleanupCount reports how many Cleanup calls were issued towards an object.
func (c *Client) CleanupCount(deviceID, feature string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cleanupCounts[k(deviceID, feature)]
}
