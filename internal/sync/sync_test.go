package sync

import (
	"context"
	"database/sql"
	"net"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/mariamesh/mariamesh/internal/apply"
	"github.com/mariamesh/mariamesh/internal/changelog"
	"github.com/mariamesh/mariamesh/internal/clock"
	"github.com/mariamesh/mariamesh/internal/store"
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

// TestSessionTransfersDeltas runs a full symmetric sync: side A holds two
// events, side B is empty. Both sides must terminate, B must apply both
// events, and both must record each other's vectors.
func TestSessionTransfersDeltas(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	originA := changelog.Origin{NodeID: "node-a", IncarnationID: "inc-a"}
	originB := changelog.Origin{NodeID: "node-b", IncarnationID: "inc-b"}

	dbA, mockA := mockDB(t)
	dbB, mockB := mockDB(t)

	// --- Side A expectations (initiator) ---
	mockA.ExpectQuery("SELECT `origin_node_id`").WillReturnRows(sqlmock.NewRows([]string{"origin_node_id", "origin_incarnation_id", "contiguous_seq"}))
	mockA.ExpectQuery("SELECT `current_seq`").WillReturnRows(sqlmock.NewRows([]string{"current_seq"}).AddRow(2))
	mockA.ExpectQuery("SELECT `meta_value`").WillReturnRows(sqlmock.NewRows([]string{"meta_value"}))
	mockA.ExpectQuery("SELECT `origin_seq`").WillReturnRows(
		sqlmock.NewRows([]string{"origin_seq", "change_id", "table_name", "row_id", "operation", "hlc_physical", "hlc_logical", "payload"}).
			AddRow(1, "c1", "device", "r1", "INSERT", 100, 0, `{"name":"R1"}`).
			AddRow(2, "c2", "device", "r1", "UPDATE", 101, 0, `{"location":"Ottawa"}`))
	mockA.ExpectQuery("SELECT `origin_node_id`").WillReturnRows(sqlmock.NewRows([]string{"origin_node_id", "origin_incarnation_id", "contiguous_seq"}))
	mockA.ExpectQuery("SELECT `current_seq`").WillReturnRows(sqlmock.NewRows([]string{"current_seq"}).AddRow(2))
	var ackSeenByA changelog.Vector
	_ = ackSeenByA
	mockA.ExpectExec("INSERT INTO `replication_progress`").WillReturnResult(sqlmock.NewResult(1, 1))
	mockA.ExpectExec("INSERT INTO `replication_progress`").WillReturnResult(sqlmock.NewResult(1, 1))

	// --- Side B expectations (responder) ---
	mockB.ExpectQuery("SELECT `origin_node_id`").WillReturnRows(sqlmock.NewRows([]string{"origin_node_id", "origin_incarnation_id", "contiguous_seq"}))
	mockB.ExpectQuery("SELECT `current_seq`").WillReturnRows(sqlmock.NewRows([]string{"current_seq"}).AddRow(0))
	nodeCols := []string{"node_id", "incarnation_id", "name", "status", "membership_epoch", "schema_version", "addr", "joined_at", "retired_at", "last_seen"}
	verCols := []string{"column_name", "hlc_physical", "hlc_logical", "origin_node_id", "origin_seq"}
	tombCols := []string{"hlc_physical", "hlc_logical", "origin_node_id", "origin_seq"}
	mockB.ExpectBegin()
	mockB.ExpectExec("SET @replication_apply = 1").WillReturnResult(sqlmock.NewResult(0, 0))
	for i := 0; i < 2; i++ {
		mockB.ExpectQuery("SELECT `node_id`").WillReturnRows(sqlmock.NewRows(nodeCols))
		mockB.ExpectExec("INSERT IGNORE INTO `replication_log`").WillReturnResult(sqlmock.NewResult(1, 1))
		mockB.ExpectQuery("SELECT `hlc_physical`").WillReturnRows(sqlmock.NewRows(tombCols))
		mockB.ExpectQuery("SELECT `column_name`").WillReturnRows(sqlmock.NewRows(verCols))
		mockB.ExpectExec("INSERT INTO `device`").WillReturnResult(sqlmock.NewResult(1, 1))
		mockB.ExpectExec("INSERT INTO `replication_field_version`").WillReturnResult(sqlmock.NewResult(1, 1))
	}
	mockB.ExpectExec("UPDATE `replication_local_state` SET `hlc_logical`").WillReturnResult(sqlmock.NewResult(0, 1))
	mockB.ExpectQuery("SELECT `contiguous_seq`").WillReturnRows(sqlmock.NewRows([]string{"contiguous_seq"}))
	mockB.ExpectQuery("SELECT `origin_seq`").WillReturnRows(sqlmock.NewRows([]string{"origin_seq"}).AddRow(1).AddRow(2))
	mockB.ExpectExec("INSERT INTO `replication_progress`").WillReturnResult(sqlmock.NewResult(1, 1))
	mockB.ExpectQuery("SELECT `origin_node_id`").WillReturnRows(
		sqlmock.NewRows([]string{"origin_node_id", "origin_incarnation_id", "contiguous_seq"}).AddRow("node-a", "inc-a", 2))
	mockB.ExpectQuery("SELECT `current_seq`").WillReturnRows(sqlmock.NewRows([]string{"current_seq"}).AddRow(0))
	mockB.ExpectExec("SET @replication_apply = NULL").WillReturnResult(sqlmock.NewResult(0, 0))
	mockB.ExpectCommit()
	mockB.ExpectExec("INSERT INTO `replication_progress`").WillReturnResult(sqlmock.NewResult(1, 1))

	envA := &Env{
		Store: store.New(dbA), Applier: apply.New(dbA, clock.New(), originA, map[string]apply.TableApply{}),
		Clock: clock.New(), Self: originA, Namespace: "test-ns", ProtocolVersion: 1,
		SchemaVersion: 1, Epoch: func() uint64 { return 0 }, BatchMaxEvents: 100, BatchMaxBytes: 1 << 20,
		RecordPeerVector: func(ctx context.Context, peer changelog.Origin, v changelog.Vector) error {
			ackSeenByA = v
			return store.SetProgressMany(ctx, dbA, peer, v)
		},
	}
	envB := &Env{
		Store: store.New(dbB), Applier: apply.New(dbB, clock.New(), originB,
			map[string]apply.TableApply{"device": {Name: "device", IDColumn: "id", Columns: map[string]bool{"name": true, "location": true}}}),
		Clock: clock.New(), Self: originB, Namespace: "test-ns", ProtocolVersion: 1,
		SchemaVersion: 1, Epoch: func() uint64 { return 0 }, BatchMaxEvents: 100, BatchMaxBytes: 1 << 20,
		RecordPeerVector: func(ctx context.Context, peer changelog.Origin, v changelog.Vector) error {
			return store.SetProgressMany(ctx, dbB, peer, v)
		},
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	dialed := make(chan net.Conn, 1)
	go func() {
		c, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			return
		}
		dialed <- c
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

	errA := make(chan error, 1)
	errB := make(chan error, 1)
	go func() { errA <- Session(ctx, dialer, envA, true) }()
	go func() { errB <- Session(ctx, accepted, envB, false) }()
	if err := <-errA; err != nil {
		t.Fatalf("initiator: %v", err)
	}
	if err := <-errB; err != nil {
		t.Fatalf("responder: %v", err)
	}
	if ackSeenByA[originA.Key()] != 2 {
		t.Fatalf("A did not learn B's vector: %v", ackSeenByA)
	}
}

// TestSessionSchemaMismatch verifies sessions refuse across versions.
func TestSessionSchemaMismatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dbA, mockA := mockDB(t)
	dbB, mockB := mockDB(t)
	for _, m := range []sqlmock.Sqlmock{mockA, mockB} {
		m.ExpectQuery("SELECT `origin_node_id`").WillReturnRows(sqlmock.NewRows([]string{"origin_node_id", "origin_incarnation_id", "contiguous_seq"}))
		m.ExpectQuery("SELECT `current_seq`").WillReturnRows(sqlmock.NewRows([]string{"current_seq"}).AddRow(0))
	}
	envA := &Env{Store: store.New(dbA), Applier: apply.New(dbA, clock.New(), changelog.Origin{NodeID: "a", IncarnationID: "i"}, nil),
		Self: changelog.Origin{NodeID: "a", IncarnationID: "i"}, Namespace: "ns", ProtocolVersion: 1, SchemaVersion: 17, Epoch: func() uint64 { return 0 }}
	envB := &Env{Store: store.New(dbB), Applier: apply.New(dbB, clock.New(), changelog.Origin{NodeID: "b", IncarnationID: "i"}, nil),
		Self: changelog.Origin{NodeID: "b", IncarnationID: "i"}, Namespace: "ns", ProtocolVersion: 1, SchemaVersion: 15, Epoch: func() uint64 { return 0 }}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, _ := net.Dial("tcp", ln.Addr().String())
		if c != nil {
			_ = c.SetDeadline(time.Now().Add(10 * time.Second))
			_ = Session(ctx, c, envA, true)
			c.Close()
		}
	}()
	accepted, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer accepted.Close()
	_ = accepted.SetDeadline(time.Now().Add(10 * time.Second))
	if err := Session(ctx, accepted, envB, false); err == nil {
		t.Fatalf("schema mismatch accepted")
	}
}
