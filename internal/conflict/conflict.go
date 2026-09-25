// Package conflict implements deterministic per-field last-write-wins
// resolution over HLC versions.
package conflict

import (
	"strings"

	"github.com/mariamesh/mariamesh/internal/clock"
)

// Version identifies the winning write of one field (or tombstone).
type Version struct {
	HLC       clock.HLC
	OriginID  string // origin node UUID string, deterministic tie-break
	OriginSeq uint64 // final tie-break for same-node same-tick writes
}

// Compare orders two versions. HLC first, then origin node ID
// (lexicographic), then origin sequence. Returns -1, 0 or +1.
func Compare(a, b Version) int {
	if c := a.HLC.Compare(b.HLC); c != 0 {
		return c
	}
	if c := strings.Compare(a.OriginID, b.OriginID); c != 0 {
		return c
	}
	switch {
	case a.OriginSeq < b.OriginSeq:
		return -1
	case a.OriginSeq > b.OriginSeq:
		return 1
	default:
		return 0
	}
}

// Wins reports whether incoming supersedes stored. Equal versions are
// idempotent no-ops (incoming does not win), which keeps re-delivery stable.
func Wins(incoming, stored Version) bool {
	return Compare(incoming, stored) > 0
}
