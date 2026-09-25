package replication

import (
	"time"

	"github.com/mariamesh/mariamesh/internal/changelog"
)

// NodeInfo describes one known cluster member.
type NodeInfo struct {
	NodeID          string
	IncarnationID   string
	Name            string
	Status          string
	Health          string // HEALTHY, SUSPECT, UNKNOWN, RETIRED
	MembershipEpoch uint64
	SchemaVersion   uint64
	Addr            string
	LastSeen        time.Time
}

// PeerInfo describes one network peer from the local address book.
type PeerInfo struct {
	NodeID string
	Addr   string
	// RTT is the last sampled smoothed RTT; zero when unknown.
	RTT time.Duration
	// Connected reports a live cached QUIC connection.
	Connected bool
	// NeedsSeed flags a peer whose history we cannot serve (it must seed).
	NeedsSeed bool
}

// PendingOrigin describes per-origin backlog: highest contiguous sequence
// versus highest stored sequence.
type PendingOrigin struct {
	Origin     string
	Contiguous uint64
	MaxStored  uint64
	Pending    uint64
}

// Status is a point-in-time snapshot of the replicator.
type Status struct {
	NodeID        string
	IncarnationID string
	State         string // JOINING, ACTIVE, RETIRED
	SchemaVersion uint64
	Epoch         uint64
	Vector        changelog.Vector
	Peers         []PeerInfo
	Nodes         []NodeInfo
	GCWatermarks  changelog.Vector
	Pending       []PendingOrigin
	StartedAt     time.Time
}
