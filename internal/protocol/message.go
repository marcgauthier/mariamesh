// Package protocol defines the replication wire protocol: length-delimited
// framing plus versioned JSON envelopes.
//
// The shape mirrors Superfly Corrosion's peer protocol (Apache-2.0):
// small versioned UniPayload/BiPayload-style envelopes carried over QUIC
// datagrams, unidirectional, and bidirectional streams, with a namespace
// (cluster) check on receipt. Encoding starts as JSON for debuggability;
// the version field reserves room for a future binary codec.
package protocol

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"github.com/mariamesh/mariamesh/internal/changelog"
)

// Version is the envelope version. Bump for incompatible wire changes.
const Version = 1

// MaxFrameBytes caps one decoded frame (batches and seed chunks are built
// to sit well below this).
const MaxFrameBytes = 32 << 20

// Stream/transport kinds.
const (
	// KindHello starts every bidi session in both directions.
	KindHello = "hello"
	// KindNeed requests missing ranges.
	KindNeed = "need"
	// KindBatch carries change events.
	KindBatch = "batch"
	// KindAck advertises the sender's vector after persisting.
	KindAck = "ack"
	// KindBroadcast is a fire-and-forget batch over a uni stream.
	KindBroadcast = "broadcast"
	// KindHeartbeat is a membership heartbeat over a datagram.
	KindHeartbeat = "heartbeat"
	// KindSeedRequest asks a peer for a full snapshot.
	KindSeedRequest = "seed-request"
	// KindSeedStart begins a snapshot with its vector.
	KindSeedStart = "seed-start"
	// KindSeedRows carries a chunk of table rows.
	KindSeedRows = "seed-rows"
	// KindSeedVersions carries a chunk of field versions.
	KindSeedVersions = "seed-versions"
	// KindSeedTombstones carries a chunk of tombstones.
	KindSeedTombstones = "seed-tombstones"
	// KindSeedEnd completes a snapshot with a checksum.
	KindSeedEnd = "seed-end"
	// KindError carries a session rejection (schema mismatch, retired, ...).
	KindError = "error"
)

// Envelope wraps every wire message.
type Envelope struct {
	V         int             `json:"v"`
	Namespace string          `json:"ns"`
	Kind      string          `json:"kind"`
	Body      json.RawMessage `json:"body"`
}

// Encode builds an envelope for kind with a JSON body.
func Encode(namespace, kind string, body any) ([]byte, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return json.Marshal(Envelope{V: Version, Namespace: namespace, Kind: kind, Body: raw})
}

// Decode parses an envelope, enforcing the version and namespace. It
// returns the envelope; use DecodeBody to unmarshal the payload.
func Decode(frame []byte, wantNamespace string) (Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(frame, &e); err != nil {
		return Envelope{}, fmt.Errorf("protocol: bad envelope: %w", err)
	}
	if e.V != Version {
		return Envelope{}, fmt.Errorf("protocol: version %d, want %d", e.V, Version)
	}
	if e.Namespace != wantNamespace {
		return Envelope{}, fmt.Errorf("protocol: namespace %q, want %q", e.Namespace, wantNamespace)
	}
	return e, nil
}

// DecodeBody unmarshals the envelope body into v.
func DecodeBody(e Envelope, v any) error {
	if err := json.Unmarshal(e.Body, v); err != nil {
		return fmt.Errorf("protocol: bad %s body: %w", e.Kind, err)
	}
	return nil
}

// WriteFrame writes one length-prefixed frame: 4-byte big-endian length
// followed by the payload.
func WriteFrame(w io.Writer, payload []byte) error {
	if len(payload) > MaxFrameBytes {
		return fmt.Errorf("protocol: frame of %d bytes exceeds %d", len(payload), MaxFrameBytes)
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

// ReadFrame reads one length-prefixed frame, enforcing MaxFrameBytes.
func ReadFrame(r io.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > MaxFrameBytes {
		return nil, fmt.Errorf("protocol: frame of %d bytes exceeds %d", n, MaxFrameBytes)
	}
	buf := make([]byte, n)
	_, err := io.ReadFull(r, buf)
	return buf, err
}

// Hello opens a bidi session. Both sides send one as their first frame.
type Hello struct {
	Protocol        int              `json:"protocol"`
	NodeID          string           `json:"node_id"`
	IncarnationID   string           `json:"incarnation_id"`
	SchemaVersion   uint64           `json:"schema_version"`
	MembershipEpoch uint64           `json:"membership_epoch"`
	Mode            string           `json:"mode"`
	Addr            string           `json:"addr,omitempty"`
	Vector          changelog.Vector `json:"vector"`
}

// RangeReq asks for [From..To] of one origin.
type RangeReq struct {
	Origin changelog.Origin `json:"origin"`
	From   uint64           `json:"from"`
	To     uint64           `json:"to"`
}

// Need requests missing history.
type Need struct {
	Ranges []RangeReq `json:"ranges"`
}

// Batch carries events for store-and-forward.
type Batch struct {
	Events []changelog.Event `json:"events"`
	// Complete marks the sender's queue for the requested ranges drained.
	Complete bool `json:"complete,omitempty"`
}

// Ack advertises the sender's persisted vector. It is only ever sent after
// the underlying MariaDB transaction commits.
type Ack struct {
	Vector changelog.Vector `json:"vector"`
}

// Broadcast is a fire-and-forget batch (uni stream, no ACK).
type Broadcast struct {
	Events []changelog.Event `json:"events"`
}

// MemberInfo gossips one known cluster member.
type MemberInfo struct {
	NodeID        string `json:"node_id"`
	IncarnationID string `json:"incarnation_id"`
	Name          string `json:"name"`
	Status        string `json:"status"`
	SchemaVersion uint64 `json:"schema_version"`
	Epoch         uint64 `json:"epoch"`
	Addr          string `json:"addr"`
}

// Heartbeat gossips liveness + membership over a datagram.
type Heartbeat struct {
	NodeID        string       `json:"node_id"`
	IncarnationID string       `json:"incarnation_id"`
	Name          string       `json:"name"`
	Status        string       `json:"status"`
	SchemaVersion uint64       `json:"schema_version"`
	Epoch         uint64       `json:"membership_epoch"`
	Addr          string       `json:"addr"`
	Members       []MemberInfo `json:"members,omitempty"`
}

// SeedRequest asks for a full snapshot.
type SeedRequest struct {
	BootstrapID          string `json:"bootstrap_id"`
	JoiningNodeID        string `json:"joining_node_id"`
	JoiningIncarnationID string `json:"joining_incarnation_id"`
	SchemaVersion        uint64 `json:"schema_version"`
}

// SeedTable describes one streamed table.
type SeedTable struct {
	Name     string   `json:"name"`
	IDColumn string   `json:"id_column"`
	Columns  []string `json:"columns"`
}

// SeedStart begins a snapshot; Vector is the consistent snapshot cursor.
type SeedStart struct {
	BootstrapID   string           `json:"bootstrap_id"`
	SchemaVersion uint64           `json:"schema_version"`
	Vector        changelog.Vector `json:"vector"`
	Tables        []SeedTable      `json:"tables"`
}

// SeedRow is one snapshot row: ID plus column values.
type SeedRow struct {
	ID     string         `json:"id"`
	Values map[string]any `json:"values"`
}

// SeedRows carries one chunk of one table.
type SeedRows struct {
	Table string    `json:"table"`
	Rows  []SeedRow `json:"rows"`
	// Done marks the table's last rows chunk.
	Done bool `json:"done,omitempty"`
}

// SeedVersion carries one stored field version.
type SeedVersion struct {
	RowID     string `json:"row_id"`
	Column    string `json:"column"`
	Physical  uint64 `json:"p"`
	Logical   uint64 `json:"l"`
	OriginID  string `json:"origin_id"`
	OriginSeq uint64 `json:"origin_seq"`
}

// SeedVersions carries one chunk of field versions for a table.
type SeedVersions struct {
	Table    string        `json:"table"`
	Versions []SeedVersion `json:"versions"`
	Done     bool          `json:"done,omitempty"`
}

// SeedTombstone carries one stored tombstone.
type SeedTombstone struct {
	RowID     string `json:"row_id"`
	Physical  uint64 `json:"p"`
	Logical   uint64 `json:"l"`
	OriginID  string `json:"origin_id"`
	OriginSeq uint64 `json:"origin_seq"`
}

// SeedTombstones carries one chunk of tombstones for a table.
type SeedTombstones struct {
	Table      string          `json:"table"`
	Tombstones []SeedTombstone `json:"tombstones"`
	Done       bool            `json:"done,omitempty"`
}

// SeedEnd completes a snapshot; Checksum covers the canonical row encoding.
type SeedEnd struct {
	BootstrapID string `json:"bootstrap_id"`
	Checksum    uint32 `json:"checksum"`
}

// Error carries a session rejection.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Error codes.
const (
	ErrCodeSchemaMismatch   = "schema_mismatch"
	ErrCodeProtocolMismatch = "protocol_mismatch"
	ErrCodeRetired          = "retired"
	ErrCodeHistoryTruncated = "history_truncated"
	ErrCodeUnknownTable     = "unknown_table"
	ErrCodeSeedNotEmpty     = "seed_not_empty"
	ErrCodeInternal         = "internal"
)
