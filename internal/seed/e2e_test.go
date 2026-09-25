package seed

import (
	"context"
	"database/sql"
	"net"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/mariamesh/mariamesh/internal/changelog"
	"github.com/mariamesh/mariamesh/internal/clock"
	"github.com/mariamesh/mariamesh/internal/protocol"
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

// TestSeedEndToEnd streams a snapshot from a mocked source to a mocked
// target over TCP and verifies the checksum handshake agrees.
func TestSeedEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	nodeCols := []string{"node_id", "incarnation_id", "name", "status", "membership_epoch", "schema_version", "addr", "joined_at", "retired_at", "last_seen"}

	dbS, mockS := mockDB(t)
	dbT, mockT := mockDB(t)

	// --- Source: Prepare ---
	mockS.ExpectQuery("SELECT `node_id`").WillReturnRows(sqlmock.NewRows(nodeCols)) // joiner unknown
	mockS.ExpectExec("INSERT INTO `replication_bootstrap`").WillReturnResult(sqlmock.NewResult(1, 1))
	mockS.ExpectBegin()
	mockS.ExpectQuery("SELECT `node_id`, `incarnation_id`, `current_seq`").WillReturnRows(
		sqlmock.NewRows([]string{"node_id", "incarnation_id", "current_seq", "hlc_physical", "hlc_logical"}).
			AddRow("src", "si", 5, 90, 0))
	mockS.ExpectQuery("SELECT `origin_node_id`").WillReturnRows(sqlmock.NewRows([]string{"origin_node_id", "origin_incarnation_id", "contiguous_seq"}))
	mockS.ExpectQuery("SELECT `current_seq`").WillReturnRows(sqlmock.NewRows([]string{"current_seq"}).AddRow(5))
	mockS.ExpectExec("INSERT INTO `replication_bootstrap_vector`").WillReturnResult(sqlmock.NewResult(1, 1))
	// --- Source: Stream ---
	mockS.ExpectQuery("SELECT COLUMN_NAME, DATA_TYPE").WillReturnRows(
		sqlmock.NewRows([]string{"COLUMN_NAME", "DATA_TYPE"}).AddRow("name", "varchar").AddRow("id", "char"))
	mockS.ExpectQuery("SELECT `id`, `name` FROM `device`").WillReturnRows(
		sqlmock.NewRows([]string{"id", "name"}).AddRow([]byte("r1"), []byte("R1")).AddRow([]byte("r2"), []byte("R2")))
	mockS.ExpectQuery("SELECT `row_id`, `column_name`").WillReturnRows(
		sqlmock.NewRows([]string{"row_id", "column_name", "hlc_physical", "hlc_logical", "origin_node_id", "origin_seq"}).
			AddRow("r1", "name", 100, 0, "a", 9))
	mockS.ExpectQuery("SELECT `row_id`, `hlc_physical`").WillReturnRows(
		sqlmock.NewRows([]string{"row_id", "hlc_physical", "hlc_logical", "origin_node_id", "origin_seq"}).
			AddRow("r9", 50, 1, "b", 2))
	mockS.ExpectCommit()
	mockS.ExpectExec("UPDATE `replication_bootstrap` SET").WillReturnResult(sqlmock.NewResult(0, 1))

	// --- Target: Fetch ---
	mockT.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0)) // device empty
	for i := 0; i < 3; i++ {
		mockT.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0))
	}
	mockT.ExpectBegin() // rows chunk
	mockT.ExpectExec("SET @replication_apply = 1").WillReturnResult(sqlmock.NewResult(0, 0))
	mockT.ExpectExec("INSERT INTO `device`").WillReturnResult(sqlmock.NewResult(1, 1))
	mockT.ExpectExec("INSERT INTO `device`").WillReturnResult(sqlmock.NewResult(1, 1))
	mockT.ExpectExec("SET @replication_apply = NULL").WillReturnResult(sqlmock.NewResult(0, 0))
	mockT.ExpectCommit()
	mockT.ExpectBegin() // versions chunk
	mockT.ExpectExec("SET @replication_apply = 1").WillReturnResult(sqlmock.NewResult(0, 0))
	mockT.ExpectExec("INSERT INTO `replication_field_version`").WillReturnResult(sqlmock.NewResult(1, 1))
	mockT.ExpectExec("SET @replication_apply = NULL").WillReturnResult(sqlmock.NewResult(0, 0))
	mockT.ExpectCommit()
	mockT.ExpectBegin() // tombstones chunk
	mockT.ExpectExec("SET @replication_apply = 1").WillReturnResult(sqlmock.NewResult(0, 0))
	mockT.ExpectExec("INSERT INTO `replication_tombstone`").WillReturnResult(sqlmock.NewResult(1, 1))
	mockT.ExpectExec("SET @replication_apply = NULL").WillReturnResult(sqlmock.NewResult(0, 0))
	mockT.ExpectCommit()
	mockT.ExpectBegin() // finish: vector + HLC
	mockT.ExpectExec("INSERT INTO `replication_progress`").WillReturnResult(sqlmock.NewResult(1, 1))
	mockT.ExpectExec("UPDATE `replication_local_state` SET `hlc_logical`").WillReturnResult(sqlmock.NewResult(0, 1))
	mockT.ExpectCommit()

	src := &Source{DB: dbS, Namespace: "ns", SchemaVersion: 1,
		Tables: []Table{{Name: "device", IDColumn: "id", Columns: []string{"name"}}}, ChunkRows: 500}
	tgt := &Target{DB: dbT, Clock: clock.New(), Self: changelog.Origin{NodeID: "dst", IncarnationID: "di"},
		Namespace: "ns", SchemaVersion: 1,
		Tables: map[string]Table{"device": {Name: "device", IDColumn: "id", Columns: []string{"name"}}}}
	req := protocol.SeedRequest{BootstrapID: "bs1", JoiningNodeID: "dst", JoiningIncarnationID: "di", SchemaVersion: 1}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	dialed := make(chan net.Conn, 1)
	go func() {
		c, err := net.Dial("tcp", ln.Addr().String())
		if err == nil {
			dialed <- c
		}
	}()
	accepted, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	dialer := <-dialed
	_ = accepted.SetDeadline(time.Now().Add(15 * time.Second))
	_ = dialer.SetDeadline(time.Now().Add(15 * time.Second))
	defer accepted.Close()
	defer dialer.Close()

	srcErr := make(chan error, 1)
	go func() {
		// Mirror production dispatch: the first frame (seed-request) is
		// consumed by the router before Serve runs.
		frame, err := protocol.ReadFrame(accepted)
		if err != nil {
			srcErr <- err
			return
		}
		e, err := protocol.Decode(frame, "ns")
		if err != nil {
			srcErr <- err
			return
		}
		var got protocol.SeedRequest
		if err := protocol.DecodeBody(e, &got); err != nil {
			srcErr <- err
			return
		}
		srcErr <- src.Serve(ctx, accepted, "", got)
	}()
	vec, err := tgt.Fetch(ctx, dialer, req)
	if err != nil {
		t.Fatalf("target: %v", err)
	}
	if err := <-srcErr; err != nil {
		t.Fatalf("source: %v", err)
	}
	if vec["src:si"] != 5 {
		t.Fatalf("seed vector = %v", vec)
	}
}

// TestTargetRefusesNonEmpty verifies seeding never touches a live database.
func TestTargetRefusesNonEmpty(t *testing.T) {
	ctx := context.Background()
	db, mock := mockDB(t)
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(7))
	tgt := &Target{DB: db, Clock: clock.New(), Namespace: "ns", SchemaVersion: 1,
		Tables: map[string]Table{"device": {Name: "device", IDColumn: "id"}}}
	if _, err := tgt.Fetch(ctx, nil, protocol.SeedRequest{}); err == nil {
		t.Fatalf("seed into non-empty database accepted")
	}
}
