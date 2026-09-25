package membership

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
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

var nodeCols = []string{"node_id", "incarnation_id", "name", "status", "membership_epoch", "schema_version", "addr", "joined_at", "retired_at", "last_seen"}

func TestHandleHeartbeatDiscoversStranger(t *testing.T) {
	db, mock := mockDB(t)
	ctx := context.Background()
	m := New(db, "ns", "self", "i", "me", 1, "127.0.0.1:1", nil)

	frame, err := protocol.Encode("ns", protocol.KindHeartbeat, protocol.Heartbeat{
		NodeID: "b", IncarnationID: "i", Name: "bee", Status: "ACTIVE",
		SchemaVersion: 1, Epoch: 5, Addr: "127.0.0.1:2",
		Members: []protocol.MemberInfo{{NodeID: "c", IncarnationID: "i", Status: "JOINING", Epoch: 5}},
	})
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectExec("INSERT INTO `replication_meta`").WillReturnResult(sqlmock.NewResult(1, 1)) // epoch 5 adopted
	mock.ExpectQuery("SELECT `node_id`").WillReturnRows(sqlmock.NewRows(nodeCols))              // b unknown
	mock.ExpectExec("INSERT INTO `replication_node`").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery("SELECT `node_id`").WillReturnRows(sqlmock.NewRows(nodeCols)) // c unknown
	mock.ExpectExec("INSERT INTO `replication_node`").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("UPDATE `replication_node` SET `last_seen`").WillReturnResult(sqlmock.NewResult(0, 1))
	if err := m.HandleHeartbeat(ctx, frame); err != nil {
		t.Fatal(err)
	}
	if m.Epoch() != 5 {
		t.Fatalf("epoch = %d, want 5", m.Epoch())
	}
}

func TestMergeMemberRetiredIrreversible(t *testing.T) {
	db, mock := mockDB(t)
	ctx := context.Background()
	m := New(db, "ns", "self", "i", "me", 1, "127.0.0.1:1", nil)

	frame, err := protocol.Encode("ns", protocol.KindHeartbeat, protocol.Heartbeat{
		NodeID: "b", IncarnationID: "i", Status: "ACTIVE", SchemaVersion: 1, Epoch: 0,
		Members: []protocol.MemberInfo{{NodeID: "r", IncarnationID: "old", Status: "ACTIVE", Epoch: 3}},
	})
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery("SELECT `node_id`").WillReturnRows(sqlmock.NewRows(nodeCols)) // b unknown
	mock.ExpectExec("INSERT INTO `replication_node`").WillReturnResult(sqlmock.NewResult(1, 1))
	// r is stored RETIRED: gossip claiming ACTIVE must be ignored (no status write).
	mock.ExpectQuery("SELECT `node_id`").WillReturnRows(sqlmock.NewRows(nodeCols).
		AddRow("r", "old", "", "RETIRED", 3, 1, "", nil, nil, nil))
	mock.ExpectExec("UPDATE `replication_node` SET `last_seen`").WillReturnResult(sqlmock.NewResult(0, 1))
	if err := m.HandleHeartbeat(ctx, frame); err != nil {
		t.Fatal(err)
	}
}

func TestAddNodeRetiredIncarnationBlocked(t *testing.T) {
	db, mock := mockDB(t)
	ctx := context.Background()
	m := New(db, "ns", "self", "i", "me", 1, "127.0.0.1:1", nil)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT `node_id`").WillReturnRows(sqlmock.NewRows(nodeCols).
		AddRow("r", "old", "", "RETIRED", 3, 1, "", nil, nil, nil))
	mock.ExpectRollback()
	if err := m.AddNode(ctx, "r", "old", "x", "", 1); err == nil {
		t.Fatalf("retired incarnation re-added")
	}
}
