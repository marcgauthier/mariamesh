package seed

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/mariamesh/mariamesh/internal/apply"
	"github.com/mariamesh/mariamesh/internal/changelog"
	"github.com/mariamesh/mariamesh/internal/clock"
	"github.com/mariamesh/mariamesh/internal/protocol"
	"github.com/mariamesh/mariamesh/internal/store"
)

// Target loads a snapshot stream into an empty database.
type Target struct {
	DB            *sql.DB
	Clock         *clock.Clock
	Self          changelog.Origin
	Namespace     string
	SchemaVersion uint64
	Tables        map[string]Table // lower(name) -> spec
}

// Fetch requests a snapshot from rw (already connected to the source) and
// applies it. The local database must be empty: every registered table, the
// log, field versions, and tombstones. It returns the seed vector.
func (t *Target) Fetch(ctx context.Context, rw io.ReadWriter, req protocol.SeedRequest) (changelog.Vector, error) {
	if err := t.requireEmpty(ctx); err != nil {
		return nil, err
	}
	if err := writeMsg(rw, t.Namespace, protocol.KindSeedRequest, req); err != nil {
		return nil, err
	}
	e, err := readEnvelope(rw, t.Namespace)
	if err != nil {
		return nil, err
	}
	if e.Kind == protocol.KindError {
		var perr protocol.Error
		_ = protocol.DecodeBody(e, &perr)
		return nil, fmt.Errorf("seed: source refused: %s: %s", perr.Code, perr.Message)
	}
	if e.Kind != protocol.KindSeedStart {
		return nil, fmt.Errorf("seed: expected seed-start, got %q", e.Kind)
	}
	var start protocol.SeedStart
	if err := protocol.DecodeBody(e, &start); err != nil {
		return nil, err
	}
	if start.SchemaVersion != t.SchemaVersion {
		return nil, fmt.Errorf("seed: schema mismatch: source=%d local=%d", start.SchemaVersion, t.SchemaVersion)
	}
	if start.BootstrapID != req.BootstrapID {
		return nil, fmt.Errorf("seed: bootstrap id mismatch")
	}
	for _, st := range start.Tables {
		if _, ok := t.Tables[strings.ToLower(st.Name)]; !ok {
			return nil, fmt.Errorf("seed: source streams unregistered table %q", st.Name)
		}
	}

	sum := NewChecksum()
	sum.Vector(start.Vector.Origins(), func(k string) uint64 { return start.Vector[k] })
	var maxHLC clock.HLC
	for _, st := range start.Tables {
		sum.Table(st.Name)
	}
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		e, err := readEnvelope(rw, t.Namespace)
		if err != nil {
			return nil, err
		}
		switch e.Kind {
		case protocol.KindSeedRows:
			var m protocol.SeedRows
			if err := protocol.DecodeBody(e, &m); err != nil {
				return nil, err
			}
			for _, r := range m.Rows {
				sum.Row(r.ID, r.Values)
			}
			if err := t.applyRows(ctx, m.Table, m.Rows); err != nil {
				return nil, err
			}
		case protocol.KindSeedVersions:
			var m protocol.SeedVersions
			if err := protocol.DecodeBody(e, &m); err != nil {
				return nil, err
			}
			tx, err := apply.BeginApplyTx(ctx, t.DB)
			if err != nil {
				return nil, err
			}
			for _, v := range m.Versions {
				sum.Version(v.RowID, v.Column, v.Physical, v.Logical, v.OriginID, v.OriginSeq)
				h := clock.HLC{Physical: v.Physical, Logical: v.Logical}
				if h.Compare(maxHLC) > 0 {
					maxHLC = h
				}
				if err := store.SetFieldVersion(ctx, tx, m.Table, v.RowID, v.Column, h, v.OriginID, v.OriginSeq); err != nil {
					apply.RollbackApplyTx(ctx, tx)
					return nil, err
				}
			}
			if err := apply.CommitApplyTx(ctx, tx); err != nil {
				return nil, err
			}
		case protocol.KindSeedTombstones:
			var m protocol.SeedTombstones
			if err := protocol.DecodeBody(e, &m); err != nil {
				return nil, err
			}
			tx, err := apply.BeginApplyTx(ctx, t.DB)
			if err != nil {
				return nil, err
			}
			for _, v := range m.Tombstones {
				sum.Tombstone(v.RowID, v.Physical, v.Logical, v.OriginID, v.OriginSeq)
				h := clock.HLC{Physical: v.Physical, Logical: v.Logical}
				if h.Compare(maxHLC) > 0 {
					maxHLC = h
				}
				if err := store.SetTombstone(ctx, tx, m.Table, v.RowID, h, v.OriginID, v.OriginSeq); err != nil {
					apply.RollbackApplyTx(ctx, tx)
					return nil, err
				}
			}
			if err := apply.CommitApplyTx(ctx, tx); err != nil {
				return nil, err
			}
		case protocol.KindSeedEnd:
			var m protocol.SeedEnd
			if err := protocol.DecodeBody(e, &m); err != nil {
				return nil, err
			}
			if m.Checksum != sum.Sum() {
				return nil, fmt.Errorf("seed: checksum mismatch: got %08x want %08x", sum.Sum(), m.Checksum)
			}
			if err := t.finish(ctx, start.Vector, maxHLC); err != nil {
				return nil, err
			}
			if err := writeMsg(rw, t.Namespace, protocol.KindAck, protocol.Ack{Vector: start.Vector}); err != nil {
				return nil, err
			}
			return start.Vector, nil
		case protocol.KindError:
			var perr protocol.Error
			_ = protocol.DecodeBody(e, &perr)
			return nil, fmt.Errorf("seed: source error %s: %s", perr.Code, perr.Message)
		default:
			return nil, fmt.Errorf("seed: unexpected message %q", e.Kind)
		}
	}
}

// requireEmpty refuses to seed into a non-empty database.
func (t *Target) requireEmpty(ctx context.Context) error {
	for _, spec := range t.Tables {
		name := spec.Name
		var n int
		if err := t.DB.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", quote(name))).Scan(&n); err != nil {
			return fmt.Errorf("seed: inspect %s: %w", name, err)
		}
		if n != 0 {
			return fmt.Errorf("seed: table %s is not empty (%d rows); seeding requires an empty database", name, n)
		}
	}
	for _, meta := range []string{"replication_log", "replication_field_version", "replication_tombstone"} {
		var n int
		if err := t.DB.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", quote(meta))).Scan(&n); err != nil {
			return fmt.Errorf("seed: inspect %s: %w", meta, err)
		}
		if n != 0 {
			return fmt.Errorf("seed: %s is not empty (%d rows); seeding requires an empty database", meta, n)
		}
	}
	return nil
}

// applyRows upserts one chunk with triggers suppressed.
func (t *Target) applyRows(ctx context.Context, table string, rows []protocol.SeedRow) error {
	if len(rows) == 0 {
		return nil
	}
	spec, ok := t.Tables[strings.ToLower(table)]
	if !ok {
		return fmt.Errorf("seed: unregistered table %q", table)
	}
	tx, err := apply.BeginApplyTx(ctx, t.DB)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			apply.RollbackApplyTx(ctx, tx)
		}
	}()
	for _, r := range rows {
		cols := make([]string, 0, len(r.Values)+1)
		marks := make([]string, 0, len(r.Values)+1)
		args := make([]any, 0, 2*len(r.Values)+1)
		cols = append(cols, quote(spec.IDColumn))
		marks = append(marks, "?")
		args = append(args, r.ID)
		keys := make([]string, 0, len(r.Values))
		for k := range r.Values {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		updates := make([]string, 0, len(keys))
		vals := make([]any, 0, len(keys))
		for _, k := range keys {
			v, err := decodeValue(r.Values[k])
			if err != nil {
				return err
			}
			cols = append(cols, quote(k))
			marks = append(marks, "?")
			args = append(args, v)
			updates = append(updates, quote(k)+" = ?")
			vals = append(vals, v)
		}
		args = append(args, vals...)
		if len(updates) == 0 {
			updates = []string{quote(spec.IDColumn) + " = " + quote(spec.IDColumn)} // id-only row: no-op update
		}
		q := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
			quote(spec.Name), strings.Join(cols, ", "), strings.Join(marks, ", "), strings.Join(updates, ", "))
		if _, err := tx.ExecContext(ctx, q, args...); err != nil {
			return fmt.Errorf("seed: insert %s/%s: %w", table, r.ID, err)
		}
	}
	if err := apply.CommitApplyTx(ctx, tx); err != nil {
		return err
	}
	committed = true
	return nil
}

// finish records the seed vector, restores the HLC past all seeded writes,
// and persists both.
func (t *Target) finish(ctx context.Context, vector changelog.Vector, maxHLC clock.HLC) error {
	tx, err := t.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := store.SetProgressMany(ctx, tx, t.Self, vector); err != nil {
		_ = tx.Rollback()
		return err
	}
	t.Clock.Restore(maxHLC)
	if err := store.SaveHLC(ctx, tx, t.Clock.Load()); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}
