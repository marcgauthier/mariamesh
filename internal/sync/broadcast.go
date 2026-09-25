package sync

import (
	"context"
	"database/sql"

	"github.com/mariamesh/mariamesh/internal/changelog"
	"github.com/mariamesh/mariamesh/internal/protocol"
	"github.com/mariamesh/mariamesh/internal/store"
)

// Broadcaster gossips fresh local events to a random peer subset for fast
// propagation. Sync sessions remain the reliable channel; broadcasts are
// hints that usually avoid waiting for the next anti-entropy round.
type Broadcaster struct {
	db        *sql.DB
	self      changelog.Origin
	namespace string
	lastSent  uint64
	started   bool
}

// NewBroadcaster returns a Broadcaster. The first Collect call anchors at
// the current sequence so restarts never rebroadcast old history.
func NewBroadcaster(db *sql.DB, self changelog.Origin, namespace string) *Broadcaster {
	return &Broadcaster{db: db, self: self, namespace: namespace}
}

// Collect fetches up to maxEvents unsent local events and advances the
// cursor past them. The caller fans the frame out to peers.
func (b *Broadcaster) Collect(ctx context.Context, maxEvents int) ([]changelog.Event, error) {
	var current uint64
	err := b.db.QueryRowContext(ctx, "SELECT `current_seq` FROM `replication_local_state` LIMIT 1").Scan(&current)
	if err != nil {
		return nil, err
	}
	if !b.started {
		b.lastSent = current
		b.started = true
		return nil, nil
	}
	if current <= b.lastSent {
		return nil, nil
	}
	events, err := store.FetchRange(ctx, b.db, b.self, b.lastSent+1, current, maxEvents, 1<<20)
	if err != nil {
		return nil, err
	}
	if len(events) > 0 {
		b.lastSent = events[len(events)-1].OriginSeq
	} else {
		b.lastSent = current
	}
	return events, nil
}

// BroadcastFrame encodes events as a uni-stream frame.
func BroadcastFrame(namespace string, events []changelog.Event) ([]byte, error) {
	payload, err := protocol.Encode(namespace, protocol.KindBroadcast, protocol.Broadcast{Events: events})
	if err != nil {
		return nil, err
	}
	return frameBytes(payload), nil
}

func frameBytes(payload []byte) []byte {
	n := len(payload)
	out := make([]byte, 4+n)
	out[0] = byte(n >> 24)
	out[1] = byte(n >> 16)
	out[2] = byte(n >> 8)
	out[3] = byte(n)
	copy(out[4:], payload)
	return out
}
