package replication

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/mariamesh/mariamesh/internal/schema"
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

// TestValidateSchemaHappyPath locks the read-only validation contract:
// metadata tables + columns, singleton local state, app tables + triggers.
func TestValidateSchemaHappyPath(t *testing.T) {
	db, mock := mockDB(t)
	ctx := context.Background()
	tables := []Table{{Name: "device", Columns: []Column{{Name: "location"}}}}

	mock.ExpectQuery("SELECT DATABASE").WillReturnRows(sqlmock.NewRows([]string{"DATABASE()"}).AddRow("repltest"))
	for _, tbl := range schema.MetadataTables {
		mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))
		rows := sqlmock.NewRows([]string{"COLUMN_NAME"})
		for _, c := range schema.RequiredColumns[tbl] {
			rows.AddRow(c)
		}
		mock.ExpectQuery("SELECT COLUMN_NAME").WillReturnRows(rows)
	}
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0)) // fresh DB: OK
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1)) // app table
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1)) // id col
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1)) // name col
	for i := 0; i < 3; i++ {
		mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))
	}
	if err := ValidateSchema(ctx, db, tables); err != nil {
		t.Fatalf("valid schema rejected: %v", err)
	}
}

func TestValidateSchemaMissingTrigger(t *testing.T) {
	db, mock := mockDB(t)
	ctx := context.Background()
	tables := []Table{{Name: "device"}}

	mock.ExpectQuery("SELECT DATABASE").WillReturnRows(sqlmock.NewRows([]string{"DATABASE()"}).AddRow("repltest"))
	for _, tbl := range schema.MetadataTables {
		mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))
		rows := sqlmock.NewRows([]string{"COLUMN_NAME"})
		for _, c := range schema.RequiredColumns[tbl] {
			rows.AddRow(c)
		}
		mock.ExpectQuery("SELECT COLUMN_NAME").WillReturnRows(rows)
	}
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0)) // update trigger missing
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))
	if err := ValidateSchema(ctx, db, tables); !errors.Is(err, ErrValidation) {
		t.Fatalf("err = %v, want validation failure", err)
	}
}
