package gc

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
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

func TestWatermarksMinAcrossActive(t *testing.T) {
	db, mock := mockDB(t)
	ctx := context.Background()
	mock.ExpectQuery("SELECT `meta_value`").WillReturnRows(sqlmock.NewRows([]string{"meta_value"}).AddRow("3"))
	mock.ExpectQuery("SELECT `node_id`").WillReturnRows(sqlmock.NewRows(nodeCols).
		AddRow("a", "i", "", "ACTIVE", 3, 1, "", nil, nil, nil).
		AddRow("b", "i", "", "ACTIVE", 3, 1, "", nil, nil, nil).
		AddRow("c", "i", "", "JOINING", 3, 1, "", nil, nil, nil).
		AddRow("d", "i", "", "RETIRED", 3, 1, "", nil, nil, nil))
	mock.ExpectQuery("SELECT `receiver_node_id`").WillReturnRows(
		sqlmock.NewRows([]string{"receiver_node_id", "receiver_incarnation_id", "origin_node_id", "origin_incarnation_id", "contiguous_seq"}).
			AddRow("a", "i", "o", "i", 10).
			AddRow("b", "i", "o", "i", 7).
			AddRow("c", "i", "o", "i", 999). // JOINING: ignored
			AddRow("d", "i", "o", "i", 999)) // RETIRED: ignored
	mock.ExpectQuery("SELECT v.`origin_node_id`").WillReturnRows(sqlmock.NewRows([]string{"origin_node_id", "origin_incarnation_id", "MIN"}))
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0))
	wm, err := Watermarks(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if wm["o:i"] != 7 {
		t.Fatalf("watermarks = %v, want o:i=7", wm)
	}
}

func TestWatermarksSeedPin(t *testing.T) {
	db, mock := mockDB(t)
	ctx := context.Background()
	mock.ExpectQuery("SELECT `meta_value`").WillReturnRows(sqlmock.NewRows([]string{"meta_value"}).AddRow("3"))
	mock.ExpectQuery("SELECT `node_id`").WillReturnRows(sqlmock.NewRows(nodeCols).
		AddRow("a", "i", "", "ACTIVE", 3, 1, "", nil, nil, nil).
		AddRow("b", "i", "", "ACTIVE", 3, 1, "", nil, nil, nil))
	mock.ExpectQuery("SELECT `receiver_node_id`").WillReturnRows(
		sqlmock.NewRows([]string{"receiver_node_id", "receiver_incarnation_id", "origin_node_id", "origin_incarnation_id", "contiguous_seq"}).
			AddRow("a", "i", "o", "i", 100).
			AddRow("b", "i", "o", "i", 100).
			AddRow("a", "i", "n", "i", 50).
			AddRow("b", "i", "n", "i", 50))
	// Seed snapshot holds o at 60; n (created after) is absent -> pinned 0.
	mock.ExpectQuery("SELECT v.`origin_node_id`").WillReturnRows(
		sqlmock.NewRows([]string{"origin_node_id", "origin_incarnation_id", "MIN"}).AddRow("o", "i", 60))
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))
	wm, err := Watermarks(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if wm["o:i"] != 60 {
		t.Fatalf("pinned origin = %v, want o:i=60", wm)
	}
	if wm["n:i"] != 0 {
		t.Fatalf("unpinned-seed origin = %v, want n:i=0 (retain all)", wm)
	}
}

func TestWatermarksStaleEpoch(t *testing.T) {
	db, mock := mockDB(t)
	ctx := context.Background()
	mock.ExpectQuery("SELECT `meta_value`").WillReturnRows(sqlmock.NewRows([]string{"meta_value"}).AddRow("3"))
	mock.ExpectQuery("SELECT `node_id`").WillReturnRows(sqlmock.NewRows(nodeCols).
		AddRow("a", "i", "", "ACTIVE", 5, 1, "", nil, nil, nil))
	if _, err := Watermarks(ctx, db); !errors.Is(err, ErrStaleMembership) {
		t.Fatalf("err = %v, want stale membership", err)
	}
}

func TestRunDeletesBounded(t *testing.T) {
	db, mock := mockDB(t)
	ctx := context.Background()
	mock.ExpectQuery("SELECT `meta_value`").WillReturnRows(sqlmock.NewRows([]string{"meta_value"}).AddRow("1"))
	mock.ExpectQuery("SELECT `node_id`").WillReturnRows(sqlmock.NewRows(nodeCols).
		AddRow("a", "i", "", "ACTIVE", 1, 1, "", nil, nil, nil))
	mock.ExpectQuery("SELECT `receiver_node_id`").WillReturnRows(
		sqlmock.NewRows([]string{"receiver_node_id", "receiver_incarnation_id", "origin_node_id", "origin_incarnation_id", "contiguous_seq"}).
			AddRow("a", "i", "o", "i", 50))
	mock.ExpectQuery("SELECT v.`origin_node_id`").WillReturnRows(sqlmock.NewRows([]string{"origin_node_id", "origin_incarnation_id", "MIN"}))
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0))
	mock.ExpectExec("DELETE FROM `replication_log`").WillReturnResult(sqlmock.NewResult(0, 500))
	mock.ExpectExec("INSERT INTO `replication_meta`").WillReturnResult(sqlmock.NewResult(1, 1))
	res, err := Run(ctx, db, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if res.Deleted != 500 || res.Watermarks["o:i"] != 50 {
		t.Fatalf("result = %+v", res)
	}
}
