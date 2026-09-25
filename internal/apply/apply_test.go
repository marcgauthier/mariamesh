package apply

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/mariamesh/mariamesh/internal/changelog"
	"github.com/mariamesh/mariamesh/internal/clock"
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

var nodeCols = []string{"node_id", "incarnation_id", "name", "status", "membership_epoch", "schema_version", "addr", "joined_at", "retired_at", "last_seen"}

func testTables() map[string]TableApply {
	return map[string]TableApply{
		"device": {Name: "device", IDColumn: "id", Columns: map[string]bool{"name": true, "location": true}},
	}
}

func insertEvent() changelog.Event {
	return changelog.Event{
		Origin:    changelog.Origin{NodeID: "peer", IncarnationID: "i"},
		OriginSeq: 6, ChangeID: "c6", Table: "device", RowID: "r1",
		Op:      changelog.OpInsert,
		HLC:     clock.HLC{Physical: 200, Logical: 0},
		Payload: map[string]any{"name": "R1"},
	}
}

// expectApplyTx expects the full successful batch skeleton around per-event
// row expectations installed by the caller between beginMarker and hlcMarker.
func beginTx(mock sqlmock.Sqlmock) {
	mock.ExpectBegin()
	mock.ExpectExec("SET @replication_apply = 1").WillReturnResult(sqlmock.NewResult(0, 0))
}

func endTx(mock sqlmock.Sqlmock, vecOrigin, vecInc string, vecSeq uint64, currentSeq uint64) {
	mock.ExpectExec("UPDATE `replication_local_state` SET `hlc_logical`").WillReturnResult(sqlmock.NewResult(0, 1))
	// AdvanceContiguous for the touched origin.
	mock.ExpectQuery("SELECT `contiguous_seq`").WillReturnRows(sqlmock.NewRows([]string{"contiguous_seq"}).AddRow(vecSeq - 1))
	mock.ExpectQuery("SELECT `origin_seq`").WillReturnRows(sqlmock.NewRows([]string{"origin_seq"}).AddRow(vecSeq))
	mock.ExpectExec("INSERT INTO `replication_progress`").WillReturnResult(sqlmock.NewResult(1, 1))
	// LocalVectorTx.
	mock.ExpectQuery("SELECT `origin_node_id`").WillReturnRows(
		sqlmock.NewRows([]string{"origin_node_id", "origin_incarnation_id", "contiguous_seq"}).AddRow(vecOrigin, vecInc, vecSeq))
	mock.ExpectQuery("SELECT `current_seq`").WillReturnRows(sqlmock.NewRows([]string{"current_seq"}).AddRow(currentSeq))
	mock.ExpectExec("SET @replication_apply = NULL").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
}

func TestApplyBatchInsert(t *testing.T) {
	db, mock := mockDB(t)
	ctx := context.Background()
	self := changelog.Origin{NodeID: "self", IncarnationID: "i"}
	a := New(db, clock.New(), self, testTables())

	beginTx(mock)
	mock.ExpectQuery("SELECT `node_id`").WillReturnRows(sqlmock.NewRows(nodeCols)) // CheckIncarnation: unknown
	mock.ExpectExec("INSERT IGNORE INTO `replication_log`").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery("SELECT `hlc_physical`").WillReturnRows(sqlmock.NewRows([]string{"hlc_physical", "hlc_logical", "origin_node_id", "origin_seq"}))               // no tombstone
	mock.ExpectQuery("SELECT `column_name`").WillReturnRows(sqlmock.NewRows([]string{"column_name", "hlc_physical", "hlc_logical", "origin_node_id", "origin_seq"})) // no versions
	mock.ExpectExec("INSERT INTO `device`").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO `replication_field_version`").WillReturnResult(sqlmock.NewResult(1, 1))
	endTx(mock, "peer", "i", 6, 0)

	res, err := a.ApplyBatch(ctx, []changelog.Event{insertEvent()})
	if err != nil {
		t.Fatal(err)
	}
	if res.Stored != 1 || res.Written != 1 {
		t.Fatalf("result = %+v", res)
	}
	if res.Vector["peer:i"] != 6 {
		t.Fatalf("vector = %v", res.Vector)
	}
}

func TestApplyBatchUsesRegisteredSpelling(t *testing.T) {
	db, mock := mockDB(t)
	ctx := context.Background()
	self := changelog.Origin{NodeID: "self", IncarnationID: "i"}
	a := New(db, clock.New(), self, testTables())

	e := insertEvent()
	e.Table = "DEVICE" // sender casing differs; registered `device` must win
	beginTx(mock)
	mock.ExpectQuery("SELECT `node_id`").WillReturnRows(sqlmock.NewRows(nodeCols))
	mock.ExpectExec("INSERT IGNORE INTO `replication_log`").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery("SELECT `hlc_physical`").WillReturnRows(sqlmock.NewRows([]string{"hlc_physical", "hlc_logical", "origin_node_id", "origin_seq"}))
	mock.ExpectQuery("SELECT `column_name`").WillReturnRows(sqlmock.NewRows([]string{"column_name", "hlc_physical", "hlc_logical", "origin_node_id", "origin_seq"}))
	mock.ExpectExec("INSERT INTO `device`").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO `replication_field_version`").WillReturnResult(sqlmock.NewResult(1, 1))
	endTx(mock, "peer", "i", 6, 0)

	if _, err := a.ApplyBatch(ctx, []changelog.Event{e}); err != nil {
		t.Fatal(err)
	}
}

func TestApplyBatchDuplicateSkipped(t *testing.T) {
	db, mock := mockDB(t)
	ctx := context.Background()
	self := changelog.Origin{NodeID: "self", IncarnationID: "i"}
	a := New(db, clock.New(), self, testTables())

	beginTx(mock)
	mock.ExpectQuery("SELECT `node_id`").WillReturnRows(sqlmock.NewRows(nodeCols))
	mock.ExpectExec("INSERT IGNORE INTO `replication_log`").WillReturnResult(sqlmock.NewResult(0, 0)) // duplicate
	endTx(mock, "peer", "i", 6, 0)

	res, err := a.ApplyBatch(ctx, []changelog.Event{insertEvent()})
	if err != nil {
		t.Fatal(err)
	}
	if res.Stored != 0 || res.Written != 0 {
		t.Fatalf("result = %+v, want all skipped", res)
	}
}

func TestApplyBatchLoserSkipped(t *testing.T) {
	db, mock := mockDB(t)
	ctx := context.Background()
	self := changelog.Origin{NodeID: "self", IncarnationID: "i"}
	a := New(db, clock.New(), self, testTables())

	beginTx(mock)
	mock.ExpectQuery("SELECT `node_id`").WillReturnRows(sqlmock.NewRows(nodeCols))
	mock.ExpectExec("INSERT IGNORE INTO `replication_log`").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery("SELECT `hlc_physical`").WillReturnRows(sqlmock.NewRows([]string{"hlc_physical", "hlc_logical", "origin_node_id", "origin_seq"}))
	// Stored version is newer than the incoming event (200): loses.
	mock.ExpectQuery("SELECT `column_name`").WillReturnRows(
		sqlmock.NewRows([]string{"column_name", "hlc_physical", "hlc_logical", "origin_node_id", "origin_seq"}).
			AddRow("name", 999, 0, "self", 3))
	endTx(mock, "peer", "i", 6, 0)

	res, err := a.ApplyBatch(ctx, []changelog.Event{insertEvent()})
	if err != nil {
		t.Fatal(err)
	}
	if res.Stored != 1 || res.Written != 0 {
		t.Fatalf("result = %+v, want stored-but-not-written", res)
	}
}

func TestApplyBatchDeleteWins(t *testing.T) {
	db, mock := mockDB(t)
	ctx := context.Background()
	self := changelog.Origin{NodeID: "self", IncarnationID: "i"}
	a := New(db, clock.New(), self, testTables())

	del := insertEvent()
	del.Op = changelog.OpDelete
	del.Payload = nil

	beginTx(mock)
	mock.ExpectQuery("SELECT `node_id`").WillReturnRows(sqlmock.NewRows(nodeCols))
	mock.ExpectExec("INSERT IGNORE INTO `replication_log`").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery("SELECT `hlc_physical`").WillReturnRows(sqlmock.NewRows([]string{"hlc_physical", "hlc_logical", "origin_node_id", "origin_seq"}))
	mock.ExpectExec("DELETE FROM `device`").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO `replication_tombstone`").WillReturnResult(sqlmock.NewResult(1, 1))
	endTx(mock, "peer", "i", 6, 0)

	res, err := a.ApplyBatch(ctx, []changelog.Event{del})
	if err != nil {
		t.Fatal(err)
	}
	if res.Stored != 1 || res.Written != 1 {
		t.Fatalf("result = %+v", res)
	}
}

func TestApplyBatchResetsFlagOnError(t *testing.T) {
	db, mock := mockDB(t)
	ctx := context.Background()
	self := changelog.Origin{NodeID: "self", IncarnationID: "i"}
	a := New(db, clock.New(), self, testTables())

	beginTx(mock)
	mock.ExpectQuery("SELECT `node_id`").WillReturnRows(sqlmock.NewRows(nodeCols))
	mock.ExpectExec("INSERT IGNORE INTO `replication_log`").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery("SELECT `hlc_physical`").WillReturnRows(sqlmock.NewRows([]string{"hlc_physical", "hlc_logical", "origin_node_id", "origin_seq"}))
	mock.ExpectQuery("SELECT `column_name`").WillReturnRows(sqlmock.NewRows([]string{"column_name", "hlc_physical", "hlc_logical", "origin_node_id", "origin_seq"}))
	mock.ExpectExec("INSERT INTO `device`").WillReturnError(errors.New("boom"))
	// Flag reset must precede rollback even on failure.
	mock.ExpectExec("SET @replication_apply = NULL").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	if _, err := a.ApplyBatch(ctx, []changelog.Event{insertEvent()}); err == nil {
		t.Fatalf("expected error")
	}
}
