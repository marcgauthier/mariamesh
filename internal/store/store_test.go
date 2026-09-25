package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/mariamesh/mariamesh/internal/changelog"
)

func mockDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
		_ = db.Close()
	})
	return db, mock
}

func TestEnsureLocalStateCreates(t *testing.T) {
	db, mock := mockDB(t)
	ctx := context.Background()
	mock.ExpectQuery("SELECT `node_id`").WillReturnRows(sqlmock.NewRows([]string{"node_id", "incarnation_id", "current_seq", "hlc_physical", "hlc_logical"}))
	mock.ExpectExec("INSERT INTO `replication_local_state`").WithArgs("n1", "i1").WillReturnResult(sqlmock.NewResult(1, 1))
	st, err := New(db).EnsureLocalState(ctx, "n1", "i1")
	if err != nil {
		t.Fatal(err)
	}
	if st.NodeID != "n1" || st.CurrentSeq != 0 {
		t.Fatalf("state = %+v", st)
	}
}

func TestEnsureLocalStateMismatch(t *testing.T) {
	db, mock := mockDB(t)
	ctx := context.Background()
	mock.ExpectQuery("SELECT `node_id`").WillReturnRows(
		sqlmock.NewRows([]string{"node_id", "incarnation_id", "current_seq", "hlc_physical", "hlc_logical"}).
			AddRow("other", "i9", 5, 1, 0))
	if _, err := New(db).EnsureLocalState(ctx, "n1", "i1"); err == nil {
		t.Fatalf("identity mismatch accepted")
	}
}

func TestInsertEventIdempotent(t *testing.T) {
	db, mock := mockDB(t)
	ctx := context.Background()
	e := changelog.Event{
		Origin: changelog.Origin{NodeID: "n", IncarnationID: "i"}, OriginSeq: 3,
		ChangeID: "c1", Table: "device", RowID: "r1", Op: changelog.OpUpdate,
		Payload: map[string]any{"name": "x"},
	}
	mock.ExpectExec("INSERT IGNORE INTO `replication_log`").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT IGNORE INTO `replication_log`").WillReturnResult(sqlmock.NewResult(0, 0))
	first, err := InsertEvent(ctx, db, e)
	if err != nil || !first {
		t.Fatalf("first insert = %v, %v", first, err)
	}
	second, err := InsertEvent(ctx, db, e)
	if err != nil || second {
		t.Fatalf("duplicate insert = %v, %v", second, err)
	}
	if err := badEvent().Validate(); err == nil {
		t.Fatalf("bad event passed validation")
	}
	if _, err := InsertEvent(ctx, db, badEvent()); err == nil {
		t.Fatalf("bad event inserted")
	}
}

func badEvent() changelog.Event { return changelog.Event{} }

func TestFetchRangeTruncated(t *testing.T) {
	db, mock := mockDB(t)
	ctx := context.Background()
	o := changelog.Origin{NodeID: "n", IncarnationID: "i"}
	mock.ExpectQuery("SELECT `meta_value`").WillReturnRows(sqlmock.NewRows([]string{"meta_value"}).AddRow("10"))
	if _, err := FetchRange(ctx, db, o, 5, 12, 100, 1<<20); !errors.Is(err, ErrHistoryTruncated) {
		t.Fatalf("err = %v, want history truncated", err)
	}
}

func TestFetchRangeMapsRows(t *testing.T) {
	db, mock := mockDB(t)
	ctx := context.Background()
	o := changelog.Origin{NodeID: "n", IncarnationID: "i"}
	mock.ExpectQuery("SELECT `meta_value`").WillReturnRows(sqlmock.NewRows([]string{"meta_value"}))
	mock.ExpectQuery("SELECT `origin_seq`").WillReturnRows(
		sqlmock.NewRows([]string{"origin_seq", "change_id", "table_name", "row_id", "operation", "hlc_physical", "hlc_logical", "payload"}).
			AddRow(11, "c1", "device", "r1", "INSERT", 100, 0, `{"name":"R1"}`).
			AddRow(12, "c2", "device", "r2", "DELETE", 101, 0, nil))
	events, err := FetchRange(ctx, db, o, 11, 20, 100, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Payload["name"] != "R1" || events[1].Op != changelog.OpDelete {
		t.Fatalf("events = %+v", events)
	}
}

func TestAdvanceContiguousFindsGap(t *testing.T) {
	db, mock := mockDB(t)
	ctx := context.Background()
	self := changelog.Origin{NodeID: "self", IncarnationID: "i"}
	remote := changelog.Origin{NodeID: "peer", IncarnationID: "i"}
	mock.ExpectQuery("SELECT `contiguous_seq`").WillReturnRows(sqlmock.NewRows([]string{"contiguous_seq"}).AddRow(5))
	mock.ExpectQuery("SELECT `origin_seq`").WillReturnRows(sqlmock.NewRows([]string{"origin_seq"}).AddRow(6).AddRow(7).AddRow(9))
	mock.ExpectExec("INSERT INTO `replication_progress`").WillReturnResult(sqlmock.NewResult(1, 1))
	cur, err := AdvanceContiguous(ctx, db, self, remote)
	if err != nil {
		t.Fatal(err)
	}
	if cur != 7 {
		t.Fatalf("contiguous = %d, want 7 (gap at 8)", cur)
	}
}

func TestGetSetProgress(t *testing.T) {
	db, mock := mockDB(t)
	ctx := context.Background()
	self := changelog.Origin{NodeID: "self", IncarnationID: "i"}
	remote := changelog.Origin{NodeID: "peer", IncarnationID: "i"}
	mock.ExpectExec("INSERT INTO `replication_progress`").WithArgs("self", "i", "peer", "i", uint64(42)).WillReturnResult(sqlmock.NewResult(1, 1))
	if err := SetProgress(ctx, db, self, remote, 42); err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery("SELECT `origin_node_id`").WillReturnRows(
		sqlmock.NewRows([]string{"origin_node_id", "origin_incarnation_id", "contiguous_seq"}).AddRow("peer", "i", 42))
	v, err := GetProgress(ctx, db, self)
	if err != nil {
		t.Fatal(err)
	}
	if v[remote.Key()] != 42 {
		t.Fatalf("vector = %v", v)
	}
}

func TestCheckIncarnation(t *testing.T) {
	db, mock := mockDB(t)
	ctx := context.Background()
	// Unknown node: allowed.
	mock.ExpectQuery("SELECT `node_id`").WillReturnRows(sqlmock.NewRows([]string{"node_id", "incarnation_id", "name", "status", "membership_epoch", "schema_version", "addr", "joined_at", "retired_at", "last_seen"}))
	if err := CheckIncarnation(ctx, db, "ghost", "i"); err != nil {
		t.Fatalf("unknown node rejected: %v", err)
	}
	// Known retired incarnation: rejected.
	mock.ExpectQuery("SELECT `node_id`").WillReturnRows(
		sqlmock.NewRows([]string{"node_id", "incarnation_id", "name", "status", "membership_epoch", "schema_version", "addr", "joined_at", "retired_at", "last_seen"}).
			AddRow("n", "old", "", "RETIRED", 3, 1, "", nil, nil, nil))
	if err := CheckIncarnation(ctx, db, "n", "old"); !errors.Is(err, ErrRetiredIncarnation) {
		t.Fatalf("err = %v", err)
	}
}

func TestActiveBootstrapPins(t *testing.T) {
	db, mock := mockDB(t)
	ctx := context.Background()
	mock.ExpectQuery("SELECT v.`origin_node_id`").WillReturnRows(
		sqlmock.NewRows([]string{"origin_node_id", "origin_incarnation_id", "MIN"}).
			AddRow("a", "i", 100).AddRow("b", "i", 50))
	pins, err := ActiveBootstrapPins(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if pins["a:i"] != 100 || pins["b:i"] != 50 {
		t.Fatalf("pins = %v", pins)
	}
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))
	has, err := HasActiveBootstrap(ctx, db)
	if err != nil || !has {
		t.Fatalf("has = %v, %v", has, err)
	}
}
