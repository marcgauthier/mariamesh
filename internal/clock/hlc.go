// Package clock implements a Hybrid Logical Clock (HLC) for causal,
// wall-clock-close versioning of replication events.
//
// An HLC timestamp is (physicalMillis, logical). Tick returns a timestamp
// greater than both the local clock and any timestamp previously observed
// via Update, which keeps causally-related events ordered even across nodes
// with skewed wall clocks.
package clock

import (
	"sync"
	"time"
)

// HLC is a hybrid logical clock timestamp. The zero value is the minimum.
type HLC struct {
	// Physical is wall time in milliseconds since the Unix epoch.
	Physical uint64 `json:"p"`
	// Logical disambiguates events within the same millisecond.
	Logical uint64 `json:"l"`
}

// Compare orders two timestamps: physical first, then logical.
// It returns -1, 0 or +1.
func (h HLC) Compare(o HLC) int {
	if h.Physical < o.Physical {
		return -1
	}
	if h.Physical > o.Physical {
		return 1
	}
	if h.Logical < o.Logical {
		return -1
	}
	if h.Logical > o.Logical {
		return 1
	}
	return 0
}

// After reports h > o. Equal reports h == o.
func (h HLC) After(o HLC) bool  { return h.Compare(o) > 0 }
func (h HLC) Equal(o HLC) bool  { return h.Compare(o) == 0 }
func (h HLC) String() string    { return time.UnixMilli(int64(h.Physical)).UTC().Format(time.RFC3339Nano) }
func (h HLC) IsZero() bool      { return h.Physical == 0 && h.Logical == 0 }
func Max(a, b HLC) HLC          { if a.Compare(b) >= 0 { return a }; return b }

// Clock is a concurrency-safe HLC generator.
type Clock struct {
	mu      sync.Mutex
	physical uint64
	logical  uint64
	now      func() uint64 // millis; override in tests
}

// New returns a Clock seeded from the current wall time.
func New() *Clock {
	return &Clock{now: func() uint64 { return uint64(time.Now().UnixMilli()) }}
}

// NewWithNow returns a Clock driven by now (millis) for tests.
func NewWithNow(now func() uint64) *Clock {
	return &Clock{now: now}
}

// Load returns the current timestamp without advancing.
func (c *Clock) Load() HLC {
	c.mu.Lock()
	defer c.mu.Unlock()
	return HLC{Physical: c.physical, Logical: c.logical}
}

// Restore sets the clock state, used at startup from replication_local_state
// so a restart never reissues timestamps.
func (c *Clock) Restore(h HLC) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if h.Physical > c.physical || (h.Physical == c.physical && h.Logical > c.logical) {
		c.physical, c.logical = h.Physical, h.Logical
	}
}

// Tick generates the next timestamp for a local event.
func (c *Clock) Tick() HLC {
	c.mu.Lock()
	defer c.mu.Unlock()
	wall := c.now()
	if wall > c.physical {
		c.physical, c.logical = wall, 0
	} else {
		c.logical++
	}
	return HLC{Physical: c.physical, Logical: c.logical}
}

// Update observes a remote timestamp and advances the clock past it,
// returning the new local time.
func (c *Clock) Update(remote HLC) HLC {
	c.mu.Lock()
	defer c.mu.Unlock()
	wall := c.now()
	switch {
	case wall > c.physical && wall > remote.Physical:
		c.physical, c.logical = wall, 0
	case c.physical > remote.Physical:
		c.logical++
	case remote.Physical > c.physical:
		c.physical, c.logical = remote.Physical, remote.Logical+1
	default: // equal physical: take max logical + 1
		if remote.Logical > c.logical {
			c.logical = remote.Logical
		}
		c.logical++
	}
	return HLC{Physical: c.physical, Logical: c.logical}
}
