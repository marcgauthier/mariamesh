// Command basic shows the minimal host integration: open MariaDB, run
// application migrations (including the replication SQL), and start the
// replicator. It needs MARIAMESH_DSN set; otherwise it prints the generated
// SQL and exits.
package main

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"

	replication "github.com/mariamesh/mariamesh"
	iquic "github.com/mariamesh/mariamesh/internal/quic"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dsn := os.Getenv("MARIAMESH_DSN")
	device := replication.Table{
		Name:       "device",
		IDColumn:   "id",
		NameColumn: "name",
		Columns:    []replication.Column{{Name: "location"}, {Name: "enabled"}},
	}
	if dsn == "" {
		fmt.Println("-- Set MARIAMESH_DSN=user:pass@tcp(127.0.0.1:3306)/mydb to run live.")
		fmt.Println("-- Metadata schema SQL:")
		fmt.Println(replication.MetadataSchemaSQL())
		fmt.Println("-- Trigger SQL for table device:")
		trg, err := replication.TriggerSQL(device)
		if err != nil {
			panic(err)
		}
		fmt.Println(trg)
		return
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		panic(err)
	}

	// In a real application the statements below live in versioned
	// migrations. They are inline here for brevity.
	mustExecFile(ctx, db, replication.MetadataSchemaSQL())
	mustExec(ctx, db, `CREATE TABLE IF NOT EXISTS device (
		id CHAR(36) NOT NULL PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		location VARCHAR(255) NULL,
		enabled TINYINT(1) NOT NULL DEFAULT 1
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	trg, err := replication.TriggerSQL(device)
	if err != nil {
		panic(err)
	}
	drop, create := splitTriggers(device.Name, trg)
	for _, stmt := range drop {
		mustExec(ctx, db, stmt)
	}
	for _, stmt := range create {
		mustExec(ctx, db, stmt)
	}

	namespace := uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
	nodeID := uuid.New()
	incarnationID := uuid.New()
	tlsConfig, err := devTLS(nodeID.String())
	if err != nil {
		panic(err)
	}

	rep, err := replication.New(replication.Config{
		DB: db, NodeID: nodeID, IncarnationID: incarnationID,
		Namespace: namespace, NodeName: "node-1", SchemaVersion: 1,
		ListenAddr: "127.0.0.1:7743", AdvertiseAddr: "127.0.0.1:7743",
		TLSConfig: tlsConfig,
	})
	if err != nil {
		panic(err)
	}
	if err := rep.RegisterTable(device); err != nil {
		panic(err)
	}
	if err := rep.Validate(ctx); err != nil {
		panic(err)
	}
	if err := rep.Start(ctx); err != nil {
		panic(err)
	}
	defer rep.Close()

	// Insert one row; the trigger assigns its replication sequence.
	id := replication.ID(namespace, "device", "Router-001")
	if _, err := db.ExecContext(ctx,
		"INSERT INTO device (id, name, location) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE name=VALUES(name)",
		id.String(), "Router-001", "Ottawa"); err != nil {
		panic(err)
	}

	status, err := rep.Status(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Printf("node=%s state=%s vector=%v peers=%d\n",
		status.NodeID, status.State, status.Vector, len(status.Peers))
}

func mustExec(ctx context.Context, db *sql.DB, stmt string) {
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		panic(fmt.Sprintf("%v\n%s", err, stmt))
	}
}

func mustExecFile(ctx context.Context, db *sql.DB, file string) {
	for _, stmt := range splitStatements(file) {
		mustExec(ctx, db, stmt)
	}
}

// splitStatements splits on ";\n" boundaries. The generated DDL never
// embeds semicolons inside string literals, so this is safe here.
func splitStatements(file string) []string {
	var out []string
	start := 0
	for i := 0; i < len(file); i++ {
		if file[i] == ';' && (i+1 == len(file) || file[i+1] == '\n') {
			out = append(out, file[start:i+1])
			start = i + 1
		}
	}
	return out
}

func splitTriggers(table, file string) (drop, create []string) {
	dropSrc := replication.DropTriggerSQL(table)
	for _, s := range splitStatements(dropSrc) {
		drop = append(drop, s)
	}
	// CREATE TRIGGER bodies contain semicolons; split on the "END;" markers.
	rest := file
	for {
		idx := indexEnd(rest)
		if idx < 0 {
			break
		}
		create = append(create, rest[:idx+len("END;")])
		rest = rest[idx+len("END;"):]
	}
	return drop, create
}

func indexEnd(s string) int {
	for i := 0; i+4 <= len(s); i++ {
		if s[i:i+4] == "END;" {
			return i
		}
	}
	return -1
}

// devTLS builds a self-signed development identity bound to nodeID.
// Production deployments must issue node certificates from their own CA
// with the node ID as a SAN.
func devTLS(nodeID string) (*tls.Config, error) {
	cert, pool, err := iquic.GenerateNodeCert(nodeID)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ClientCAs:    pool,
		ClientAuth:   tls.VerifyClientCertIfGiven,
		ServerName:   nodeID,
	}, nil
}
