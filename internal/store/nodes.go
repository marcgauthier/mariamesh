package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Node statuses.
const (
	StatusJoining = "JOINING"
	StatusActive  = "ACTIVE"
	StatusRetired = "RETIRED"
)

// Node mirrors a replication_node row.
type Node struct {
	NodeID          string
	IncarnationID   string
	Name            string
	Status          string
	MembershipEpoch uint64
	SchemaVersion   uint64
	Addr            string
	JoinedAt        time.Time
	RetiredAt       time.Time
	LastSeen        time.Time
}

// UpsertNode inserts or refreshes a node record (heartbeats refresh
// last_seen; status/epoch move forward only via dedicated transitions).
func UpsertNode(ctx context.Context, q qtx, n Node) error {
	_, err := q.ExecContext(ctx,
		`INSERT INTO `+"`replication_node`"+` (`+"`node_id`"+`, `+"`incarnation_id`"+`, `+"`name`"+`, `+"`status`"+`,
		  `+"`membership_epoch`"+`, `+"`schema_version`"+`, `+"`addr`"+`, `+"`joined_at`"+`, `+"`last_seen`"+`)
		 VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6))
		 ON DUPLICATE KEY UPDATE `+"`incarnation_id`"+` = VALUES(`+"`incarnation_id`"+`),
			`+"`name`"+` = VALUES(`+"`name`"+`), `+"`schema_version`"+` = VALUES(`+"`schema_version`"+`),
			`+"`addr`"+` = VALUES(`+"`addr`"+`), `+"`last_seen`"+` = CURRENT_TIMESTAMP(6),
			`+"`membership_epoch`"+` = GREATEST(`+"`membership_epoch`"+`, VALUES(`+"`membership_epoch`"+`))`,
		n.NodeID, n.IncarnationID, n.Name, n.Status, n.MembershipEpoch, n.SchemaVersion, n.Addr)
	return err
}

// GetNode reads one node by ID.
func GetNode(ctx context.Context, q qtx, nodeID string) (Node, error) {
	var n Node
	var joined, retired, seen sql.NullTime
	err := q.QueryRowContext(ctx,
		"SELECT `node_id`, `incarnation_id`, `name`, `status`, `membership_epoch`, `schema_version`, `addr`, `joined_at`, `retired_at`, `last_seen` FROM `replication_node` WHERE `node_id` = ?",
		nodeID).Scan(&n.NodeID, &n.IncarnationID, &n.Name, &n.Status, &n.MembershipEpoch, &n.SchemaVersion, &n.Addr, &joined, &retired, &seen)
	if err != nil {
		return Node{}, err
	}
	n.JoinedAt, n.RetiredAt, n.LastSeen = joined.Time, retired.Time, seen.Time
	return n, nil
}

// ListNodes returns all known nodes ordered by ID.
func ListNodes(ctx context.Context, q qtx) ([]Node, error) {
	rows, err := q.QueryContext(ctx,
		"SELECT `node_id`, `incarnation_id`, `name`, `status`, `membership_epoch`, `schema_version`, `addr`, `joined_at`, `retired_at`, `last_seen` FROM `replication_node` ORDER BY `node_id`")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Node
	for rows.Next() {
		var n Node
		var joined, retired, seen sql.NullTime
		if err := rows.Scan(&n.NodeID, &n.IncarnationID, &n.Name, &n.Status, &n.MembershipEpoch, &n.SchemaVersion, &n.Addr, &joined, &retired, &seen); err != nil {
			return nil, err
		}
		n.JoinedAt, n.RetiredAt, n.LastSeen = joined.Time, retired.Time, seen.Time
		out = append(out, n)
	}
	return out, rows.Err()
}

// SetNodeStatus moves a node to a new status at the given epoch.
func SetNodeStatus(ctx context.Context, q qtx, nodeID, status string, epoch uint64) error {
	var cmd string
	switch status {
	case StatusRetired:
		cmd = "UPDATE `replication_node` SET `status` = ?, `membership_epoch` = ?, `retired_at` = CURRENT_TIMESTAMP(6) WHERE `node_id` = ?"
	case StatusActive:
		cmd = "UPDATE `replication_node` SET `status` = ?, `membership_epoch` = ? WHERE `node_id` = ?"
	default:
		cmd = "UPDATE `replication_node` SET `status` = ?, `membership_epoch` = ? WHERE `node_id` = ?"
	}
	_, err := q.ExecContext(ctx, cmd, status, epoch, nodeID)
	return err
}

// TouchNode refreshes last_seen.
func TouchNode(ctx context.Context, q qtx, nodeID string) error {
	_, err := q.ExecContext(ctx, "UPDATE `replication_node` SET `last_seen` = CURRENT_TIMESTAMP(6) WHERE `node_id` = ?", nodeID)
	return err
}

// ErrRetiredIncarnation is returned when an event arrives from a retired
// incarnation: it must be dropped, never applied or forwarded.
var ErrRetiredIncarnation = errors.New("store: event from retired incarnation")

// CheckIncarnation rejects events whose (node, incarnation) is known-retired
// or whose node is known under a different, non-retired incarnation while
// this incarnation was explicitly retired.
func CheckIncarnation(ctx context.Context, q qtx, nodeID, incarnationID string) error {
	n, err := GetNode(ctx, q, nodeID)
	if err == sql.ErrNoRows {
		return nil // unknown node: data may precede membership; allow
	}
	if err != nil {
		return err
	}
	if n.IncarnationID == incarnationID && n.Status == StatusRetired {
		return fmt.Errorf("%w: %s/%s", ErrRetiredIncarnation, nodeID, incarnationID)
	}
	return nil
}

// GetMeta reads a replication_meta value.
func GetMeta(ctx context.Context, q qtx, key string) (string, error) {
	var v string
	err := q.QueryRowContext(ctx, "SELECT `meta_value` FROM `replication_meta` WHERE `meta_key` = ?", key).Scan(&v)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return v, err
}

// SetMeta writes a replication_meta value.
func SetMeta(ctx context.Context, q qtx, key, value string) error {
	_, err := q.ExecContext(ctx,
		"INSERT INTO `replication_meta` (`meta_key`, `meta_value`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `meta_value` = VALUES(`meta_value`)",
		key, value)
	return err
}

// EpochKey is the meta key holding the cluster membership epoch.
const EpochKey = "membership_epoch"

// GetEpoch reads the membership epoch (0 when unset).
func GetEpoch(ctx context.Context, q qtx) (uint64, error) {
	v, err := GetMeta(ctx, q, EpochKey)
	if err != nil || v == "" {
		return 0, err
	}
	var n uint64
	_, _ = fmt.Sscanf(v, "%d", &n)
	return n, nil
}

// SetEpoch writes the membership epoch.
func SetEpoch(ctx context.Context, q qtx, epoch uint64) error {
	return SetMeta(ctx, q, EpochKey, fmt.Sprintf("%d", epoch))
}

// Bootstrap mirrors a replication_bootstrap row.
type Bootstrap struct {
	ID                   string
	JoiningNodeID        string
	JoiningIncarnationID string
	Status               string
	SchemaVersion        uint64
}

// Bootstrap statuses.
const (
	BootstrapStarted   = "STARTED"
	BootstrapCompleted = "COMPLETED"
	BootstrapAborted   = "ABORTED"
)

// CreateBootstrap records a new seed session.
func CreateBootstrap(ctx context.Context, q qtx, b Bootstrap) error {
	_, err := q.ExecContext(ctx,
		"INSERT INTO `replication_bootstrap` (`bootstrap_id`, `joining_node_id`, `joining_incarnation_id`, `status`, `schema_version`) VALUES (?, ?, ?, ?, ?)",
		b.ID, b.JoiningNodeID, b.JoiningIncarnationID, b.Status, b.SchemaVersion)
	return err
}

// SetBootstrapVector records one origin's seed cursor (GC retention pin).
func SetBootstrapVector(ctx context.Context, q qtx, bootstrapID, originNodeID, originIncarnationID string, seedSeq uint64) error {
	_, err := q.ExecContext(ctx,
		"INSERT INTO `replication_bootstrap_vector` (`bootstrap_id`, `origin_node_id`, `origin_incarnation_id`, `seed_seq`) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE `seed_seq` = VALUES(`seed_seq`)",
		bootstrapID, originNodeID, originIncarnationID, seedSeq)
	return err
}

// FinishBootstrap marks a session completed/aborted.
func FinishBootstrap(ctx context.Context, q qtx, bootstrapID, status string) error {
	_, err := q.ExecContext(ctx,
		"UPDATE `replication_bootstrap` SET `status` = ?, `completed_at` = CURRENT_TIMESTAMP(6) WHERE `bootstrap_id` = ?",
		status, bootstrapID)
	return err
}

// HasActiveBootstrap reports whether any STARTED seed session exists.
func HasActiveBootstrap(ctx context.Context, q qtx) (bool, error) {
	var n int
	err := q.QueryRowContext(ctx, "SELECT COUNT(*) FROM `replication_bootstrap` WHERE `status` = 'STARTED'").Scan(&n)
	return n > 0, err
}

// ActiveBootstrapPins returns the seed vectors of all STARTED sessions.
// GC treats each as an extra ACK vector so seeding never stalls cleanup.
func ActiveBootstrapPins(ctx context.Context, q qtx) (map[string]uint64, error) {
	rows, err := q.QueryContext(ctx,
		`SELECT v.`+"`origin_node_id`"+`, v.`+"`origin_incarnation_id`"+`, MIN(v.`+"`seed_seq`"+`)
		 FROM `+"`replication_bootstrap_vector`"+` v
		 JOIN `+"`replication_bootstrap`"+` b ON b.`+"`bootstrap_id`"+` = v.`+"`bootstrap_id`"+`
		 WHERE b.`+"`status`"+` = 'STARTED'
		 GROUP BY v.`+"`origin_node_id`"+`, v.`+"`origin_incarnation_id`"+``)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]uint64{}
	for rows.Next() {
		var node, inc string
		var seq uint64
		if err := rows.Scan(&node, &inc, &seq); err != nil {
			return nil, err
		}
		out[node+":"+inc] = seq
	}
	return out, rows.Err()
}
