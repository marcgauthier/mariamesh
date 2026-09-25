// Package membership tracks cluster members over QUIC datagram heartbeats
// and the replication_node table.
//
// Heartbeats gossip (sender, member list); receivers merge by epoch with two
// iron rules: retirement is irreversible, and a higher membership epoch
// always wins. Failure suspicion is derived from last_seen staleness at read
// time, never stored. This is SWIM-lite: no active probing in v1, so
// heartbeats plus the sync path carry all membership state.
package membership

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/mariamesh/mariamesh/internal/protocol"
	"github.com/mariamesh/mariamesh/internal/store"
)

// Logger is a minimal structured logger.
type Logger interface {
	Debug(msg string, kv ...any)
	Info(msg string, kv ...any)
	Warn(msg string, kv ...any)
	Error(msg string, kv ...any)
}

// Manager owns membership state transitions.
type Manager struct {
	db            *sql.DB
	namespace     string
	nodeID        string
	incarnationID string
	name          string
	schemaVersion uint64
	addr          string
	epoch         atomic.Uint64
	log           Logger
}

// New returns a Manager. LoadEpoch must be called (or epoch 0 adopted) before use.
func New(db *sql.DB, namespace, nodeID, incarnationID, name string, schemaVersion uint64, addr string, log Logger) *Manager {
	if log == nil {
		log = noopLogger{}
	}
	return &Manager{db: db, namespace: namespace, nodeID: nodeID, incarnationID: incarnationID, name: name, schemaVersion: schemaVersion, addr: addr, log: log}
}

// LoadEpoch reads the persisted epoch into memory.
func (m *Manager) LoadEpoch(ctx context.Context) error {
	n, err := store.GetEpoch(ctx, m.db)
	if err != nil {
		return err
	}
	m.epoch.Store(n)
	return nil
}

// Epoch returns the in-memory cluster membership epoch.
func (m *Manager) Epoch() uint64 { return m.epoch.Load() }

// setEpoch persists and caches a higher epoch.
func (m *Manager) setEpoch(ctx context.Context, n uint64) error {
	for {
		cur := m.epoch.Load()
		if n <= cur {
			return nil
		}
		if m.epoch.CompareAndSwap(cur, n) {
			return store.SetEpoch(ctx, m.db, n)
		}
	}
}

// bumpEpoch increments the epoch for an administrative change and returns it.
func (m *Manager) bumpEpoch(ctx context.Context, tx *sql.Tx) (uint64, error) {
	cur, err := store.GetEpoch(ctx, tx)
	if err != nil {
		return 0, err
	}
	next := cur + 1
	if err := store.SetEpoch(ctx, tx, next); err != nil {
		return 0, err
	}
	m.epoch.Store(next)
	return next, nil
}

// SelfStatus returns this node's stored status.
func (m *Manager) SelfStatus(ctx context.Context) (string, error) {
	n, err := store.GetNode(ctx, m.db, m.nodeID)
	if err == sql.ErrNoRows {
		return store.StatusJoining, nil
	}
	return n.Status, err
}

// EnsureSelf inserts the local node row when missing.
func (m *Manager) EnsureSelf(ctx context.Context, status string) error {
	_, err := store.GetNode(ctx, m.db, m.nodeID)
	if err == nil {
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	return store.UpsertNode(ctx, m.db, store.Node{
		NodeID: m.nodeID, IncarnationID: m.incarnationID, Name: m.name,
		Status: status, MembershipEpoch: m.Epoch(), SchemaVersion: m.schemaVersion, Addr: m.addr,
	})
}

// AddNode registers (or re-registers after retirement) a node and bumps the
// epoch. Re-adding a RETIRED node ID with a new incarnation moves it back to
// JOINING so the rebuilt machine can seed; the retired incarnation itself
// can never return.
func (m *Manager) AddNode(ctx context.Context, nodeID, incarnationID, name, addr string, schemaVersion uint64) error {
	var next uint64
	err := withTx(ctx, m.db, func(tx *sql.Tx) error {
		n, err := store.GetNode(ctx, tx, nodeID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil && n.Status == store.StatusRetired && n.IncarnationID == incarnationID {
			return fmt.Errorf("membership: incarnation %s/%s is retired and cannot rejoin", nodeID, incarnationID)
		}
		status := store.StatusJoining
		var e error
		next, e = m.bumpEpoch(ctx, tx)
		if e != nil {
			return e
		}
		if err == nil && n.Status != store.StatusRetired {
			return fmt.Errorf("membership: node %s already known (%s)", nodeID, n.Status)
		}
		return store.UpsertNode(ctx, tx, store.Node{
			NodeID: nodeID, IncarnationID: incarnationID, Name: name,
			Status: status, MembershipEpoch: next, SchemaVersion: schemaVersion, Addr: addr,
		})
	})
	if err == nil {
		m.log.Info("membership: node added", "node", nodeID, "epoch", next)
	}
	return err
}

// Activate moves a node to ACTIVE at a bumped epoch.
func (m *Manager) Activate(ctx context.Context, nodeID string) error {
	return withTx(ctx, m.db, func(tx *sql.Tx) error {
		n, err := store.GetNode(ctx, tx, nodeID)
		if err != nil {
			return err
		}
		if n.Status == store.StatusRetired {
			return fmt.Errorf("membership: node %s is retired", nodeID)
		}
		if n.Status == store.StatusActive {
			return nil
		}
		next, err := m.bumpEpoch(ctx, tx)
		if err != nil {
			return err
		}
		return store.SetNodeStatus(ctx, tx, nodeID, store.StatusActive, next)
	})
}

// RetireNode irreversibly retires a node ID's current incarnation.
func (m *Manager) RetireNode(ctx context.Context, nodeID string) error {
	return withTx(ctx, m.db, func(tx *sql.Tx) error {
		if _, err := store.GetNode(ctx, tx, nodeID); err != nil {
			return err
		}
		next, err := m.bumpEpoch(ctx, tx)
		if err != nil {
			return err
		}
		return store.SetNodeStatus(ctx, tx, nodeID, store.StatusRetired, next)
	})
}

// Heartbeat builds this node's gossip payload. The member list is capped so
// the datagram stays small; larger clusters converge over several beats and
// through sync-time HELLO exchange.
func (m *Manager) Heartbeat(ctx context.Context) ([]byte, error) {
	status, err := m.SelfStatus(ctx)
	if err != nil {
		return nil, err
	}
	nodes, err := store.ListNodes(ctx, m.db)
	if err != nil {
		return nil, err
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].NodeID < nodes[j].NodeID })
	const maxMembers = 32
	members := make([]protocol.MemberInfo, 0, len(nodes))
	for _, n := range nodes {
		if n.NodeID == m.nodeID {
			continue
		}
		members = append(members, protocol.MemberInfo{
			NodeID: n.NodeID, IncarnationID: n.IncarnationID, Name: n.Name,
			Status: n.Status, SchemaVersion: n.SchemaVersion, Epoch: n.MembershipEpoch, Addr: n.Addr,
		})
		if len(members) >= maxMembers {
			break
		}
	}
	hb := protocol.Heartbeat{
		NodeID: m.nodeID, IncarnationID: m.incarnationID, Name: m.name,
		Status: status, SchemaVersion: m.schemaVersion, Epoch: m.Epoch(), Addr: m.addr, Members: members,
	}
	return protocol.Encode(m.namespace, protocol.KindHeartbeat, hb)
}

// HandleHeartbeat merges one received heartbeat. Strangers are auto-added as
// JOINING (discovery) but never auto-activated; retirement always wins.
func (m *Manager) HandleHeartbeat(ctx context.Context, frame []byte) error {
	e, err := protocol.Decode(frame, m.namespace)
	if err != nil {
		return err
	}
	if e.Kind != protocol.KindHeartbeat {
		return fmt.Errorf("membership: unexpected datagram %q", e.Kind)
	}
	var hb protocol.Heartbeat
	if err := protocol.DecodeBody(e, &hb); err != nil {
		return err
	}
	if hb.NodeID == "" || hb.NodeID == m.nodeID {
		return nil
	}
	if hb.Epoch > m.Epoch() {
		if err := m.setEpoch(ctx, hb.Epoch); err != nil {
			return err
		}
	}
	entries := append([]protocol.MemberInfo{{
		NodeID: hb.NodeID, IncarnationID: hb.IncarnationID, Name: hb.Name,
		Status: hb.Status, SchemaVersion: hb.SchemaVersion, Epoch: hb.Epoch, Addr: hb.Addr,
	}}, hb.Members...)
	for _, en := range entries {
		if en.NodeID == "" || en.NodeID == m.nodeID {
			continue
		}
		if err := m.mergeMember(ctx, en); err != nil {
			return err
		}
	}
	return store.TouchNode(ctx, m.db, hb.NodeID)
}

// mergeMember applies one gossiped member record.
func (m *Manager) mergeMember(ctx context.Context, en protocol.MemberInfo) error {
	stored, err := store.GetNode(ctx, m.db, en.NodeID)
	if errors.Is(err, sql.ErrNoRows) {
		status := en.Status
		if status == store.StatusActive {
			// Trust-but-verify: a node claiming ACTIVE still needs our
			// admin plane to activate it before it influences GC.
			status = store.StatusJoining
		}
		if status != store.StatusJoining && status != store.StatusRetired {
			status = store.StatusJoining
		}
		return store.UpsertNode(ctx, m.db, store.Node{
			NodeID: en.NodeID, IncarnationID: en.IncarnationID, Name: en.Name,
			Status: status, MembershipEpoch: en.Epoch, SchemaVersion: en.SchemaVersion, Addr: en.Addr,
		})
	}
	if err != nil {
		return err
	}
	if stored.Status == store.StatusRetired {
		return nil // irreversible, never overwritten by gossip
	}
	if en.IncarnationID != stored.IncarnationID && en.Status == store.StatusRetired {
		return nil // retirement of an incarnation we never knew; ignore
	}
	switch {
	case en.Epoch > stored.MembershipEpoch:
		if en.Status == store.StatusActive {
			// Adopt data but not activation; activation is administrative.
			return store.UpsertNode(ctx, m.db, store.Node{
				NodeID: en.NodeID, IncarnationID: en.IncarnationID, Name: en.Name,
				Status: stored.Status, MembershipEpoch: en.Epoch, SchemaVersion: en.SchemaVersion, Addr: en.Addr,
			})
		}
		return store.SetNodeStatus(ctx, m.db, en.NodeID, en.Status, en.Epoch)
	case en.Epoch == stored.MembershipEpoch && en.Status == store.StatusRetired:
		return store.SetNodeStatus(ctx, m.db, en.NodeID, store.StatusRetired, en.Epoch)
	default:
		if en.Addr != "" && en.Addr != stored.Addr {
			return store.UpsertNode(ctx, m.db, store.Node{
				NodeID: stored.NodeID, IncarnationID: stored.IncarnationID, Name: stored.Name,
				Status: stored.Status, MembershipEpoch: stored.MembershipEpoch,
				SchemaVersion: stored.SchemaVersion, Addr: en.Addr,
			})
		}
		return nil
	}
}

// CheckPeer validates a HELLO sender against retirement records.
func (m *Manager) CheckPeer(ctx context.Context, h protocol.Hello) error {
	n, err := store.GetNode(ctx, m.db, h.NodeID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if n.IncarnationID == h.IncarnationID && n.Status == store.StatusRetired {
		return fmt.Errorf("membership: incarnation %s/%s is retired", h.NodeID, h.IncarnationID)
	}
	return nil
}

// StaleEpoch reports whether any known node carries a newer epoch than
// local, in which case GC must pause (membership view is behind).
func StaleEpoch(nodes []store.Node, localEpoch uint64) bool {
	for _, n := range nodes {
		if n.MembershipEpoch > localEpoch {
			return true
		}
	}
	return false
}

// SuspectAfter is the staleness threshold deriving SUSPECT health.
const SuspectAfter = 3

// Health derives per-node liveness from last_seen.
func Health(n store.Node, heartbeatInterval time.Duration, now time.Time) string {
	if n.Status == store.StatusRetired {
		return "RETIRED"
	}
	if n.LastSeen.IsZero() {
		return "UNKNOWN"
	}
	if now.Sub(n.LastSeen) > SuspectAfter*heartbeatInterval {
		return "SUSPECT"
	}
	return "HEALTHY"
}

func withTx(ctx context.Context, db *sql.DB, fn func(tx *sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

type noopLogger struct{}

func (noopLogger) Debug(string, ...any) {}
func (noopLogger) Info(string, ...any)  {}
func (noopLogger) Warn(string, ...any)  {}
func (noopLogger) Error(string, ...any) {}
