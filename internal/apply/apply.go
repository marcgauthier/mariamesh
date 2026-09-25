// Package apply executes incoming replication events against MariaDB with
// per-field last-write-wins resolution.
//
// Every batch runs in one transaction on a dedicated connection with the
// connection-local @replication_apply flag set, so generated triggers skip
// replicated writes (they must not re-enter the log). The flag is always
// reset before the connection returns to the pool — on commit and on
// rollback paths alike, because user variables are not transactional.
package apply

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/mariamesh/mariamesh/internal/changelog"
	"github.com/mariamesh/mariamesh/internal/clock"
	"github.com/mariamesh/mariamesh/internal/conflict"
	"github.com/mariamesh/mariamesh/internal/store"
)

// TableApply describes how to write one registered table.
type TableApply struct {
	// Name is the registered table name used for SQL.
	Name string
	// IDColumn is the primary key column.
	IDColumn string
	// Columns is the allowlist of writable value columns.
	Columns map[string]bool
}

// Applier applies remote event batches.
type Applier struct {
	db     *sql.DB
	clock  *clock.Clock
	self   changelog.Origin
	tables map[string]TableApply // lower(table) -> spec
}

// New returns an Applier. tables snapshots the table registry; unknown
// tables are stored and forwarded but never written.
func New(db *sql.DB, c *clock.Clock, self changelog.Origin, tables map[string]TableApply) *Applier {
	return &Applier{db: db, clock: c, self: self, tables: tables}
}

// BeginApplyTx starts a transaction with @replication_apply set on its
// connection. Pair with CommitApplyTx or RollbackApplyTx, which reset the
// flag before releasing the connection.
func BeginApplyTx(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, "SET @replication_apply = 1"); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return tx, nil
}

// CommitApplyTx resets the apply flag and commits.
func CommitApplyTx(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, "SET @replication_apply = NULL"); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// RollbackApplyTx resets the apply flag and rolls back.
func RollbackApplyTx(ctx context.Context, tx *sql.Tx) {
	_, _ = tx.ExecContext(ctx, "SET @replication_apply = NULL")
	_ = tx.Rollback()
}

// Result summarizes a batch application.
type Result struct {
	// Stored counts events newly stored in replication_log.
	Stored int
	// Written counts events that changed a business row.
	Written int
	// Vector is the local vector after the batch committed.
	Vector changelog.Vector
}

// ApplyBatch stores and applies events atomically, then advances progress.
// Events should arrive grouped and ascending per origin, but any order is
// safe: log insertion is idempotent and cursors recompute from gaps.
func (a *Applier) ApplyBatch(ctx context.Context, events []changelog.Event) (Result, error) {
	var res Result
	if len(events) == 0 {
		v, err := a.localVector(ctx)
		if err != nil {
			return res, err
		}
		res.Vector = v
		return res, nil
	}
	tx, err := BeginApplyTx(ctx, a.db)
	if err != nil {
		return res, err
	}
	committed := false
	defer func() {
		if !committed {
			RollbackApplyTx(ctx, tx)
		}
	}()
	touched := map[string]changelog.Origin{}
	for i := range events {
		e := events[i]
		if err := e.Validate(); err != nil {
			return res, fmt.Errorf("apply: event %d invalid: %w", i, err)
		}
		if err := store.CheckIncarnation(ctx, tx, e.Origin.NodeID, e.Origin.IncarnationID); err != nil {
			return res, err
		}
		inserted, err := store.InsertEvent(ctx, tx, e)
		if err != nil {
			return res, err
		}
		touched[e.Origin.Key()] = e.Origin
		if !inserted {
			continue // duplicate delivery: log + row already reflect it
		}
		res.Stored++
		a.clock.Update(e.HLC)
		wrote, err := a.applyRow(ctx, tx, e)
		if err != nil {
			return res, err
		}
		if wrote {
			res.Written++
		}
	}
	if err := store.SaveHLC(ctx, tx, a.clock.Load()); err != nil {
		return res, err
	}
	for _, o := range touched {
		if _, err := store.AdvanceContiguous(ctx, tx, a.self, o); err != nil {
			return res, err
		}
	}
	vec, err := store.LocalVectorTx(ctx, tx, a.self)
	if err != nil {
		return res, err
	}
	if err := CommitApplyTx(ctx, tx); err != nil {
		return res, err
	}
	committed = true
	res.Vector = vec
	return res, nil
}

func (a *Applier) localVector(ctx context.Context) (changelog.Vector, error) {
	return store.LocalVectorTx(ctx, a.db, a.self)
}

func versionOf(e changelog.Event) conflict.Version {
	return conflict.Version{HLC: e.HLC, OriginID: e.Origin.NodeID, OriginSeq: e.OriginSeq}
}

func storedVersion(v store.FieldVersion) conflict.Version {
	return conflict.Version{HLC: v.HLC, OriginID: v.OriginID, OriginSeq: v.OriginSeq}
}

// applyRow resolves one event against field versions and tombstones.
// Unknown tables are forwarded-only (stored, not written).
func (a *Applier) applyRow(ctx context.Context, tx *sql.Tx, e changelog.Event) (bool, error) {
	spec, ok := a.tables[strings.ToLower(e.Table)]
	if !ok {
		return false, nil
	}
	table := spec.Name // registered spelling, not the sender's
	incoming := versionOf(e)
	tomb, hasTomb, err := store.GetTombstone(ctx, tx, table, e.RowID)
	if err != nil {
		return false, err
	}

	switch e.Op {
	case changelog.OpDelete:
		if hasTomb && !conflict.Wins(incoming, storedVersion(tomb)) {
			return false, nil
		}
		if err := deleteRow(ctx, tx, table, spec.IDColumn, e.RowID); err != nil {
			return false, err
		}
		if err := store.SetTombstone(ctx, tx, table, e.RowID, e.HLC, e.Origin.NodeID, e.OriginSeq); err != nil {
			return false, err
		}
		return true, nil
	default: // INSERT / UPDATE share field-level resolution
		if hasTomb && !conflict.Wins(incoming, storedVersion(tomb)) {
			return false, nil // an older write racing a newer delete
		}
		versions, err := store.GetFieldVersions(ctx, tx, table, e.RowID)
		if err != nil {
			return false, err
		}
		winners := map[string]any{}
		for col, val := range e.Payload {
			if !spec.Columns[col] {
				continue // not a registered column: ignore
			}
			if sv, ok := versions[col]; ok && !conflict.Wins(incoming, storedVersion(sv)) {
				continue
			}
			winners[col] = normalizeValue(val)
		}
		if len(winners) == 0 {
			return false, nil
		}
		if err := upsertRow(ctx, tx, table, spec.IDColumn, e.RowID, winners); err != nil {
			return false, err
		}
		for col := range winners {
			if err := store.SetFieldVersion(ctx, tx, table, e.RowID, col, e.HLC, e.Origin.NodeID, e.OriginSeq); err != nil {
				return false, err
			}
		}
		if hasTomb {
			// A newer write resurrects the row (deterministic IDs make
			// delete→recreate cycles address the same row).
			if err := store.ClearTombstone(ctx, tx, table, e.RowID); err != nil {
				return false, err
			}
		}
		return true, nil
	}
}

func quote(ident string) string {
	return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
}

func deleteRow(ctx context.Context, tx *sql.Tx, table, idCol, rowID string) error {
	_, err := tx.ExecContext(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE %s = ?", quote(table), quote(idCol)), rowID)
	return err
}

func upsertRow(ctx context.Context, tx *sql.Tx, table, idCol, rowID string, winners map[string]any) error {
	cols := make([]string, 0, len(winners))
	for c := range winners {
		cols = append(cols, c)
	}
	sort.Strings(cols)
	names := make([]string, 0, len(cols)+1)
	marks := make([]string, 0, len(cols)+1)
	args := make([]any, 0, 2*len(cols)+1)
	names = append(names, quote(idCol))
	marks = append(marks, "?")
	args = append(args, rowID)
	updates := make([]string, 0, len(cols))
	for _, c := range cols {
		names = append(names, quote(c))
		marks = append(marks, "?")
		args = append(args, winners[c])
		updates = append(updates, quote(c)+" = ?")
	}
	for _, c := range cols {
		args = append(args, winners[c])
	}
	q := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
		quote(table), strings.Join(names, ", "), strings.Join(marks, ", "), strings.Join(updates, ", "))
	_, err := tx.ExecContext(ctx, q, args...)
	return err
}

// normalizeValue converts JSON-decoded payload values into driver-friendly
// scalars: integral floats become int64 (safe for INT columns), bools become
// 0/1 (safe for TINYINT), and nested values re-encode as JSON text (for
// JSON/TEXT columns holding documents).
func normalizeValue(v any) any {
	switch t := v.(type) {
	case float64:
		if t == math.Trunc(t) && t >= -9e15 && t <= 9e15 {
			return int64(t)
		}
		return t
	case bool:
		if t {
			return int64(1)
		}
		return int64(0)
	case map[string]any, []any:
		b, err := json.Marshal(t)
		if err != nil {
			return fmt.Sprintf("%v", t)
		}
		return string(b)
	default:
		return v
	}
}
