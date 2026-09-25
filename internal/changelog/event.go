// Package changelog defines replication events, origins, and replication
// vectors (per-origin contiguous-sequence cursors).
package changelog

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/mariamesh/mariamesh/internal/clock"
)

// Operation is the change kind carried by an Event.
type Operation string

const (
	OpInsert Operation = "INSERT"
	OpUpdate Operation = "UPDATE"
	OpDelete Operation = "DELETE"
)

// Valid reports whether o is a known operation.
func (o Operation) Valid() bool {
	return o == OpInsert || o == OpUpdate || o == OpDelete
}

// Origin identifies one event log: the node + incarnation that produced it.
// An incarnation's sequence space is append-only starting at 1.
type Origin struct {
	NodeID        string `json:"node_id"`
	IncarnationID string `json:"incarnation_id"`
}

// Key returns the canonical map key "node:incarnation".
func (o Origin) Key() string { return o.NodeID + ":" + o.IncarnationID }

// ParseKey splits a Key back into an Origin.
func ParseKey(key string) (Origin, error) {
	node, inc, ok := strings.Cut(key, ":")
	if !ok || node == "" || inc == "" {
		return Origin{}, fmt.Errorf("changelog: invalid origin key %q", key)
	}
	return Origin{NodeID: node, IncarnationID: inc}, nil
}

// Event is one replicated row change. The identity
// (Origin, OriginSeq) is globally unique and the deduplication key.
type Event struct {
	Origin    Origin    `json:"origin"`
	OriginSeq uint64    `json:"origin_seq"`
	ChangeID  string    `json:"change_id"`
	Table     string    `json:"table"`
	RowID     string    `json:"row_id"`
	Op        Operation `json:"op"`
	HLC       clock.HLC `json:"hlc"`
	// Payload maps column -> new value for INSERT/UPDATE. UPDATE carries
	// only changed columns. Values are JSON-decoded scalars (nil allowed).
	// DELETE carries a nil payload.
	Payload map[string]any `json:"payload,omitempty"`
}

// Validate checks the event's structural invariants.
func (e Event) Validate() error {
	if e.Origin.NodeID == "" || e.Origin.IncarnationID == "" {
		return fmt.Errorf("changelog: event missing origin")
	}
	if e.OriginSeq == 0 {
		return fmt.Errorf("changelog: event origin_seq must be >= 1")
	}
	if e.Table == "" || e.RowID == "" {
		return fmt.Errorf("changelog: event missing table/row")
	}
	if !e.Op.Valid() {
		return fmt.Errorf("changelog: invalid operation %q", e.Op)
	}
	if e.Op == OpDelete && len(e.Payload) != 0 {
		return fmt.Errorf("changelog: delete must not carry a payload")
	}
	return nil
}

// PayloadJSON encodes the payload for storage.
func (e Event) PayloadJSON() (string, error) {
	if len(e.Payload) == 0 {
		return "", nil
	}
	b, err := json.Marshal(e.Payload)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// ParsePayload decodes a stored payload; empty input yields nil.
func ParsePayload(s string) (map[string]any, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return nil, err
	}
	return m, nil
}

// Vector maps origin key -> highest contiguous sequence known locally.
// Contiguous means every seq in [1..N] is present; N is the sync cursor.
type Vector map[string]uint64

// Get returns the cursor for o (0 when unknown).
func (v Vector) Get(o Origin) uint64 { return v[o.Key()] }

// Set moves the cursor for o forward only.
func (v Vector) Set(o Origin, seq uint64) {
	if seq > v[o.Key()] {
		v[o.Key()] = seq
	}
}

// Clone returns a copy.
func (v Vector) Clone() Vector {
	out := make(Vector, len(v))
	for k, n := range v {
		out[k] = n
	}
	return out
}

// Origins returns sorted origin keys for deterministic iteration.
func (v Vector) Origins() []string {
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Range describes one missing interval [From..To] for an origin.
type Range struct {
	Origin Origin
	From   uint64 // inclusive, >= 1
	To     uint64 // inclusive, >= From
}

// Missing compares have (local) against want (peer's advertisement) and
// returns the intervals the peer has that local lacks. want > have yields
// [have+1 .. want] per origin.
func Missing(have, want Vector) []Range {
	var out []Range
	for _, key := range want.Origins() {
		w := want[key]
		h := have[key]
		if w > h {
			o, err := ParseKey(key)
			if err != nil {
				continue
			}
			out = append(out, Range{Origin: o, From: h + 1, To: w})
		}
	}
	return out
}

// Min returns the per-origin minimum of the given vectors over the union of
// their origins. Origins absent from a vector count as 0 in that vector,
// which is the safe choice for GC watermarks.
func Min(vectors ...Vector) Vector {
	keys := map[string]bool{}
	for _, v := range vectors {
		for k := range v {
			keys[k] = true
		}
	}
	out := make(Vector, len(keys))
	for k := range keys {
		min := uint64(0)
		first := true
		for _, v := range vectors {
			n := v[k] // absent reads 0
			if first || n < min {
				min = n
				first = false
			}
		}
		out[k] = min
	}
	return out
}
