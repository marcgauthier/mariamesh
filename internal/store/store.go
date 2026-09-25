// Package store persists replication metadata in MariaDB: the change log,
// field versions, tombstones, progress cursors, membership, bootstrap
// sessions, and the local sequence/HLC state.
//
// It executes DML only. Schema creation belongs to the host application.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/mariamesh/mariamesh/internal/changelog"
	"github.com/mariamesh/mariamesh/internal/clock"
)

// qtx abstracts *sql.DB and *sql.Tx for query helpers.
type qtx interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Store wraps a *sql.DB with replication queries.
type Store struct {
	db *sql.DB
}

// New returns a Store over db.
func New(db *sql.DB) *Store { return &Store{db: db} }

// DB exposes the underlying handle for transactions managed by callers.
func (s *Store) DB() *sql.DB { return s.db }

// WithTx runs fn inside a transaction with the given options.
func (s *Store) WithTx(ctx context.Context, opts *sql.TxOptions, fn func(tx *sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, opts)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// ErrHistoryTruncated is returned when a peer requests sequences that were
// already garbage-collected; the peer needs a full seed instead of deltas.
var ErrHistoryTruncated = errors.New("store: requested history was garbage-collected")

// LocalState mirrors replication_local_state.
type LocalState struct {
	NodeID        string
	IncarnationID string
	CurrentSeq    uint64
	HLC           clock.HLC
}

// EnsureLocalState creates the singleton row when missing (fresh database)
// and verifies it matches this node's identity. A mismatch means the data
// directory was copied or reused across nodes/incarnations and is fatal.
func (s *Store) EnsureLocalState(ctx context.Context, nodeID, incarnationID string) (LocalState, error) {
	var st LocalState
	err := s.db.QueryRowContext(ctx,
		"SELECT `node_id`, `incarnation_id`, `current_seq`, `hlc_physical`, `hlc_logical` FROM `replication_local_state` LIMIT 1").
		Scan(&st.NodeID, &st.IncarnationID, &st.CurrentSeq, &st.HLC.Physical, &st.HLC.Logical)
	if err == sql.ErrNoRows {
		if _, err := s.db.ExecContext(ctx,
			"INSERT INTO `replication_local_state` (`node_id`, `incarnation_id`, `current_seq`, `hlc_physical`, `hlc_logical`) VALUES (?, ?, 0, 0, 0)",
			nodeID, incarnationID); err != nil {
			return LocalState{}, fmt.Errorf("store: init local state: %w", err)
		}
		return LocalState{NodeID: nodeID, IncarnationID: incarnationID}, nil
	}
	if err != nil {
		return LocalState{}, fmt.Errorf("store: load local state: %w", err)
	}
	if st.NodeID != nodeID || st.IncarnationID != incarnationID {
		return LocalState{}, fmt.Errorf("store: local state belongs to node %s incarnation %s, this node is %s/%s (data directory mismatch)",
			st.NodeID, st.IncarnationID, nodeID, incarnationID)
	}
	return st, nil
}

// LoadLocalState reads the singleton row.
func (s *Store) LoadLocalState(ctx context.Context) (LocalState, error) {
	return LoadLocalStateTx(ctx, s.db)
}

// LoadLocalStateTx reads the singleton row over an explicit querier
// (snapshot transactions).
func LoadLocalStateTx(ctx context.Context, q qtx) (LocalState, error) {
	var st LocalState
	err := q.QueryRowContext(ctx,
		"SELECT `node_id`, `incarnation_id`, `current_seq`, `hlc_physical`, `hlc_logical` FROM `replication_local_state` LIMIT 1").
		Scan(&st.NodeID, &st.IncarnationID, &st.CurrentSeq, &st.HLC.Physical, &st.HLC.Logical)
	if err != nil {
		return LocalState{}, fmt.Errorf("store: load local state: %w", err)
	}
	return st, nil
}

// SaveHLC persists clock state (monotonic max) in one atomic statement.
// Assignments evaluate left to right, so the logical update still sees the
// OLD physical value: a physical advance resets logical to 0, equal
// physicals take the max logical, and older physicals keep stored state.
func SaveHLC(ctx context.Context, q qtx, h clock.HLC) error {
	_, err := q.ExecContext(ctx,
		`UPDATE `+"`replication_local_state`"+` SET `+"`hlc_logical`"+` = CASE
		    WHEN ? > `+"`hlc_physical`"+` THEN 0
		    WHEN ? = `+"`hlc_physical`"+` THEN GREATEST(`+"`hlc_logical`"+`, ?)
		    ELSE `+"`hlc_logical`"+` END,
		  `+"`hlc_physical`"+` = GREATEST(`+"`hlc_physical`"+`, ?)`,
		h.Physical, h.Physical, h.Logical, h.Physical)
	return err
}

// AllocateSeq transactionally bumps current_seq; used by non-trigger paths
// (tests, tools). Normal application writes allocate through triggers.
func (s *Store) AllocateSeq(ctx context.Context) (uint64, error) {
	var seq uint64
	err := s.WithTx(ctx, nil, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, "UPDATE `replication_local_state` SET `current_seq` = `current_seq` + 1"); err != nil {
			return err
		}
		return tx.QueryRowContext(ctx, "SELECT `current_seq` FROM `replication_local_state` LIMIT 1").Scan(&seq)
	})
	return seq, err
}

// InsertEvent stores an event; a duplicate (origin, seq) is a no-op
// returning inserted=false, which makes re-delivery idempotent.
func InsertEvent(ctx context.Context, q qtx, e changelog.Event) (inserted bool, err error) {
	if err := e.Validate(); err != nil {
		return false, err
	}
	payload, err := e.PayloadJSON()
	if err != nil {
		return false, err
	}
	var arg any
	if payload == "" {
		arg = nil
	} else {
		arg = payload
	}
	res, err := q.ExecContext(ctx,
		`INSERT IGNORE INTO `+"`replication_log`"+`
		 (`+"`origin_node_id`"+`, `+"`origin_incarnation_id`"+`, `+"`origin_seq`"+`, `+"`change_id`"+`,
		  `+"`table_name`"+`, `+"`row_id`"+`, `+"`operation`"+`, `+"`hlc_physical`"+`, `+"`hlc_logical`"+`, `+"`payload`"+`)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		e.Origin.NodeID, e.Origin.IncarnationID, e.OriginSeq, e.ChangeID,
		e.Table, e.RowID, string(e.Op), e.HLC.Physical, e.HLC.Logical, arg)
	if err != nil {
		return false, fmt.Errorf("store: insert event: %w", err)
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// HasEvent reports whether (origin, seq) is present in the log.
func HasEvent(ctx context.Context, q qtx, o changelog.Origin, seq uint64) (bool, error) {
	var n int
	err := q.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM `replication_log` WHERE `origin_node_id` = ? AND `origin_incarnation_id` = ? AND `origin_seq` = ?",
		o.NodeID, o.IncarnationID, seq).Scan(&n)
	return n > 0, err
}

// FetchRange reads events for one origin in [from..to], ascending, bounded by
// maxEvents and maxBytes (estimated). It returns ErrHistoryTruncated when the
// request reaches into garbage-collected history.
func FetchRange(ctx context.Context, q qtx, o changelog.Origin, from, to uint64, maxEvents, maxBytes int) ([]changelog.Event, error) {
	if from > to {
		return nil, nil
	}
	deletedThrough, err := gcWatermark(ctx, q, o)
	if err != nil {
		return nil, err
	}
	if from <= deletedThrough {
		return nil, fmt.Errorf("%w: origin %s from=%d gc-through=%d", ErrHistoryTruncated, o.Key(), from, deletedThrough)
	}
	rows, err := q.QueryContext(ctx,
		`SELECT `+"`origin_seq`"+`, `+"`change_id`"+`, `+"`table_name`"+`, `+"`row_id`"+`, `+"`operation`"+`,
			`+"`hlc_physical`"+`, `+"`hlc_logical`"+`, `+"`payload`"+`
		 FROM `+"`replication_log`"+`
		 WHERE `+"`origin_node_id`"+` = ? AND `+"`origin_incarnation_id`"+` = ? AND `+"`origin_seq`"+` BETWEEN ? AND ?
		 ORDER BY `+"`origin_seq`"+` ASC`,
		o.NodeID, o.IncarnationID, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []changelog.Event
	budget := 0
	for rows.Next() {
		var e changelog.Event
		var op string
		var payload sql.NullString
		e.Origin = o
		if err := rows.Scan(&e.OriginSeq, &e.ChangeID, &e.Table, &e.RowID, &op, &e.HLC.Physical, &e.HLC.Logical, &payload); err != nil {
			return nil, err
		}
		e.Op = changelog.Operation(op)
		if payload.Valid {
			m, err := changelog.ParsePayload(payload.String)
			if err != nil {
				return nil, fmt.Errorf("store: corrupt payload at %s/%d: %w", o.Key(), e.OriginSeq, err)
			}
			e.Payload = m
		}
		budget += len(payload.String) + 128
		out = append(out, e)
		if maxEvents > 0 && len(out) >= maxEvents {
			break
		}
		if maxBytes > 0 && budget >= maxBytes {
			break
		}
	}
	return out, rows.Err()
}

// DeleteThrough removes log rows for one origin with seq <= through, in a
// bounded batch. It returns the number of rows deleted. The persisted GC
// watermark advances to through only when the batch proves nothing remains
// (deleted < limit); otherwise a later cycle finishes the range. This keeps
// FetchRange's truncation guard exact: through is advertised only when every
// row at or below it is really gone.
func DeleteThrough(ctx context.Context, q qtx, o changelog.Origin, through uint64, limit int) (int64, error) {
	if limit <= 0 {
		limit = 1000
	}
	res, err := q.ExecContext(ctx,
		"DELETE FROM `replication_log` WHERE `origin_node_id` = ? AND `origin_incarnation_id` = ? AND `origin_seq` <= ? LIMIT ?",
		o.NodeID, o.IncarnationID, through, limit)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	if n < int64(limit) {
		// Fewer than a full batch (possibly zero): nothing at or below
		// through remains, so the watermark may advance.
		if err := setGCWatermark(ctx, q, o, through); err != nil {
			return n, err
		}
	}
	return n, nil
}

func gcMetaKey(o changelog.Origin) string { return "gc:" + o.Key() }

func gcWatermark(ctx context.Context, q qtx, o changelog.Origin) (uint64, error) {
	var v string
	err := q.QueryRowContext(ctx, "SELECT `meta_value` FROM `replication_meta` WHERE `meta_key` = ?", gcMetaKey(o)).Scan(&v)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var n uint64
	_, _ = fmt.Sscanf(strings.TrimSpace(v), "%d", &n)
	return n, nil
}

func setGCWatermark(ctx context.Context, q qtx, o changelog.Origin, through uint64) error {
	_, err := q.ExecContext(ctx,
		"INSERT INTO `replication_meta` (`meta_key`, `meta_value`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `meta_value` = VALUES(`meta_value`)",
		gcMetaKey(o), fmt.Sprintf("%d", through))
	return err
}

// GCWatermarks returns the persisted per-origin GC through-sequences.
func GCWatermarks(ctx context.Context, q qtx) (changelog.Vector, error) {
	rows, err := q.QueryContext(ctx, "SELECT `meta_key`, `meta_value` FROM `replication_meta` WHERE `meta_key` LIKE 'gc:%'")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := changelog.Vector{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		var n uint64
		_, _ = fmt.Sscanf(strings.TrimSpace(v), "%d", &n)
		out[strings.TrimPrefix(k, "gc:")] = n
	}
	return out, rows.Err()
}
