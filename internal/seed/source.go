package seed

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"

	"github.com/mariamesh/mariamesh/internal/changelog"
	"github.com/mariamesh/mariamesh/internal/protocol"
	"github.com/mariamesh/mariamesh/internal/store"
)

// Source serves a consistent snapshot to a joining node.
//
// The caller must exclude garbage collection between the snapshot read and
// the pin write inside Serve (in-process: hold one mutex around both); after
// the pins persist, GC and seeding proceed concurrently.
type Source struct {
	DB            *sql.DB
	Namespace     string
	SchemaVersion uint64
	Tables        []Table
	ChunkRows     int
}

// Snapshot is a prepared seed: validated request, open snapshot transaction,
// persisted retention pins, and the snapshot vector.
type Snapshot struct {
	req    protocol.SeedRequest
	vector changelog.Vector
	tx     *sql.Tx
}

// Serve snapshots and streams in one call. Prefer Prepare + Stream when the
// caller must serialize preparation with garbage collection.
func (s *Source) Serve(ctx context.Context, rw io.ReadWriter, peerNodeID string, req protocol.SeedRequest) error {
	snap, err := s.Prepare(ctx, rw, peerNodeID, req)
	if err != nil {
		return err
	}
	return s.Stream(ctx, rw, snap)
}

// Prepare validates the request, records the bootstrap session, opens the
// consistent snapshot, and persists the GC retention pins. peerNodeID is the
// certificate-authenticated identity of the caller and must match the
// request's joining node.
//
// The caller must exclude garbage collection for the duration of Prepare;
// Stream then runs concurrently with GC.
func (s *Source) Prepare(ctx context.Context, rw io.Writer, peerNodeID string, req protocol.SeedRequest) (*Snapshot, error) {
	fail := func(code, msg string, err error) (*Snapshot, error) {
		_ = writeMsg(rw, s.Namespace, protocol.KindError, protocol.Error{Code: code, Message: msg})
		return nil, err
	}
	if req.SchemaVersion != s.SchemaVersion {
		return fail(protocol.ErrCodeSchemaMismatch, "schema mismatch",
			fmt.Errorf("seed: schema mismatch: peer=%d local=%d", req.SchemaVersion, s.SchemaVersion))
	}
	if peerNodeID != "" && req.JoiningNodeID != peerNodeID {
		return fail(protocol.ErrCodeInternal, "caller identity mismatch",
			fmt.Errorf("seed: caller %q requested seed for %q", peerNodeID, req.JoiningNodeID))
	}
	if req.BootstrapID == "" || req.JoiningNodeID == "" || req.JoiningIncarnationID == "" {
		return nil, fmt.Errorf("seed: request missing identity")
	}
	if n, err := store.GetNode(ctx, s.DB, req.JoiningNodeID); err == nil {
		if n.Status == store.StatusRetired && n.IncarnationID == req.JoiningIncarnationID {
			return fail(protocol.ErrCodeRetired, "incarnation retired", fmt.Errorf("seed: joining incarnation is retired"))
		}
		if n.Status == store.StatusActive {
			return fail(protocol.ErrCodeInternal, "node already active", fmt.Errorf("seed: node %s is already active", req.JoiningNodeID))
		}
	}

	if err := store.CreateBootstrap(ctx, s.DB, store.Bootstrap{
		ID: req.BootstrapID, JoiningNodeID: req.JoiningNodeID,
		JoiningIncarnationID: req.JoiningIncarnationID, Status: store.BootstrapStarted, SchemaVersion: req.SchemaVersion,
	}); err != nil {
		return nil, fmt.Errorf("seed: create bootstrap: %w", err)
	}

	// Consistent snapshot: REPEATABLE READ + first read defines the view.
	snap, err := s.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		_ = store.FinishBootstrap(ctx, s.DB, req.BootstrapID, store.BootstrapAborted)
		return nil, err
	}
	self, err := store.LoadLocalStateTx(ctx, snap)
	if err != nil {
		snap.Rollback()
		_ = store.FinishBootstrap(ctx, s.DB, req.BootstrapID, store.BootstrapAborted)
		return nil, err
	}
	vector, err := store.LocalVectorTx(ctx, snap, changelog.Origin{NodeID: self.NodeID, IncarnationID: self.IncarnationID})
	if err != nil {
		snap.Rollback()
		_ = store.FinishBootstrap(ctx, s.DB, req.BootstrapID, store.BootstrapAborted)
		return nil, err
	}
	// Retention pins BEFORE streaming: GC must treat the joiner as having
	// exactly this vector from here on.
	for _, key := range vector.Origins() {
		o, err := changelog.ParseKey(key)
		if err != nil {
			continue
		}
		if err := store.SetBootstrapVector(ctx, s.DB, req.BootstrapID, o.NodeID, o.IncarnationID, vector[key]); err != nil {
			snap.Rollback()
			_ = store.FinishBootstrap(ctx, s.DB, req.BootstrapID, store.BootstrapAborted)
			return nil, err
		}
	}
	return &Snapshot{req: req, vector: vector, tx: snap}, nil
}

// Stream sends a prepared snapshot and completes the bootstrap on target ACK.
func (s *Source) Stream(ctx context.Context, rw io.ReadWriter, snap *Snapshot) error {
	completed := false
	defer func() {
		if !completed {
			snap.tx.Rollback()
			_ = store.FinishBootstrap(ctx, s.DB, snap.req.BootstrapID, store.BootstrapAborted)
		}
	}()
	vector := snap.vector
	sum := NewChecksum()
	sum.Vector(vector.Origins(), func(k string) uint64 { return vector[k] })
	tables := make([]protocol.SeedTable, 0, len(s.Tables))
	for _, t := range s.Tables {
		tables = append(tables, protocol.SeedTable{Name: t.Name, IDColumn: t.IDColumn, Columns: t.Columns})
	}
	if err := writeMsg(rw, s.Namespace, protocol.KindSeedStart, protocol.SeedStart{
		BootstrapID: snap.req.BootstrapID, SchemaVersion: s.SchemaVersion, Vector: vector, Tables: tables,
	}); err != nil {
		return err
	}

	chunk := s.ChunkRows
	if chunk <= 0 {
		chunk = 500
	}
	for _, t := range s.Tables {
		sum.Table(t.Name)
		if err := s.streamRows(ctx, snap.tx, rw, t, chunk, sum); err != nil {
			return err
		}
		if err := s.streamVersions(ctx, snap.tx, rw, t, chunk, sum); err != nil {
			return err
		}
		if err := s.streamTombstones(ctx, snap.tx, rw, t, chunk, sum); err != nil {
			return err
		}
	}
	if err := snap.tx.Commit(); err != nil {
		return err
	}
	if err := writeMsg(rw, s.Namespace, protocol.KindSeedEnd, protocol.SeedEnd{BootstrapID: snap.req.BootstrapID, Checksum: sum.Sum()}); err != nil {
		return err
	}

	// Wait for the target's ACK before releasing the retention pin.
	e, err := readEnvelope(rw, s.Namespace)
	if err != nil {
		return fmt.Errorf("seed: wait for target ack: %w", err)
	}
	if e.Kind != protocol.KindAck {
		return fmt.Errorf("seed: expected ack, got %q", e.Kind)
	}
	if err := store.FinishBootstrap(ctx, s.DB, snap.req.BootstrapID, store.BootstrapCompleted); err != nil {
		return err
	}
	completed = true
	return nil
}

func quote(ident string) string {
	return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
}

func (s *Source) streamRows(ctx context.Context, snap *sql.Tx, w io.Writer, t Table, chunk int, sum *Checksum) error {
	bin, err := columnTypes(ctx, snap, t.Name)
	if err != nil {
		return err
	}
	cols := append([]string{t.IDColumn}, t.Columns...)
	sel := make([]string, 0, len(cols))
	for _, c := range cols {
		sel = append(sel, quote(c))
	}
	rows, err := snap.QueryContext(ctx,
		fmt.Sprintf("SELECT %s FROM %s ORDER BY %s", strings.Join(sel, ", "), quote(t.Name), quote(t.IDColumn)))
	if err != nil {
		return err
	}
	defer rows.Close()
	batch := make([]protocol.SeedRow, 0, chunk)
	flush := func(done bool) error {
		if len(batch) == 0 && !done {
			return nil
		}
		for _, r := range batch {
			sum.Row(r.ID, r.Values)
		}
		err := writeMsg(w, s.Namespace, protocol.KindSeedRows, protocol.SeedRows{Table: t.Name, Rows: batch, Done: done})
		batch = batch[:0]
		return err
	}
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for rows.Next() {
		for i := range vals {
			vals[i] = nil
		}
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		id, _ := vals[0].([]byte)
		row := protocol.SeedRow{ID: string(id), Values: map[string]any{}}
		for i, c := range t.Columns {
			row.Values[c] = encodeValue(vals[i+1], bin[c])
		}
		// Non-[]byte IDs (integer PKs should not happen; be lenient).
		if row.ID == "" && vals[0] != nil {
			row.ID = fmt.Sprintf("%v", encodeValue(vals[0], false))
		}
		batch = append(batch, row)
		if len(batch) >= chunk {
			if err := flush(false); err != nil {
				return err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return flush(true)
}

func (s *Source) streamVersions(ctx context.Context, snap *sql.Tx, w io.Writer, t Table, chunk int, sum *Checksum) error {
	all, err := store.AllFieldVersions(ctx, snap, t.Name)
	if err != nil {
		return err
	}
	batch := make([]protocol.SeedVersion, 0, chunk)
	flush := func(done bool) error {
		if len(batch) == 0 && !done {
			return nil
		}
		for _, v := range batch {
			sum.Version(v.RowID, v.Column, v.Physical, v.Logical, v.OriginID, v.OriginSeq)
		}
		err := writeMsg(w, s.Namespace, protocol.KindSeedVersions, protocol.SeedVersions{Table: t.Name, Versions: batch, Done: done})
		batch = batch[:0]
		return err
	}
	for _, v := range all {
		batch = append(batch, protocol.SeedVersion{RowID: v.RowID, Column: v.Column, Physical: v.HLC.Physical, Logical: v.HLC.Logical, OriginID: v.OriginID, OriginSeq: v.OriginSeq})
		if len(batch) >= chunk {
			if err := flush(false); err != nil {
				return err
			}
		}
	}
	return flush(true)
}

func (s *Source) streamTombstones(ctx context.Context, snap *sql.Tx, w io.Writer, t Table, chunk int, sum *Checksum) error {
	all, err := store.AllTombstones(ctx, snap, t.Name)
	if err != nil {
		return err
	}
	batch := make([]protocol.SeedTombstone, 0, chunk)
	flush := func(done bool) error {
		if len(batch) == 0 && !done {
			return nil
		}
		for _, v := range batch {
			sum.Tombstone(v.RowID, v.Physical, v.Logical, v.OriginID, v.OriginSeq)
		}
		err := writeMsg(w, s.Namespace, protocol.KindSeedTombstones, protocol.SeedTombstones{Table: t.Name, Tombstones: batch, Done: done})
		batch = batch[:0]
		return err
	}
	for _, v := range all {
		batch = append(batch, protocol.SeedTombstone{RowID: v.RowID, Physical: v.HLC.Physical, Logical: v.HLC.Logical, OriginID: v.OriginID, OriginSeq: v.OriginSeq})
		if len(batch) >= chunk {
			if err := flush(false); err != nil {
				return err
			}
		}
	}
	return flush(true)
}
