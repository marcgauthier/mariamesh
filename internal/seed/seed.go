// Package seed implements full state transfer for joining nodes.
//
// The source opens a consistent InnoDB snapshot (REPEATABLE READ), reads the
// replication vector plus every registered table's rows, field versions, and
// tombstones, and streams them over one bidirectional QUIC stream. Field
// versions and tombstones travel with the rows so later deltas resolve
// correctly against seeded state. A CRC32 checksum over the canonical stream
// encoding is verified by the target before it advertises the seed vector.
package seed

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/mariamesh/mariamesh/internal/protocol"
)

// Table is the seeder's view of a replicated table.
type Table struct {
	Name     string
	IDColumn string
	Columns  []string // value columns in seed order
}

// BinaryMarker wraps binary column values so the target restores raw bytes
// instead of UTF-8 text.
type BinaryMarker struct {
	Bytes string `json:"$bytes"`
}

// encodeValue converts a scanned driver value into JSON-safe form.
// isBinary selects base64 wrapping for BLOB/BINARY columns.
func encodeValue(v any, isBinary bool) any {
	switch t := v.(type) {
	case nil:
		return nil
	case []byte:
		if isBinary {
			return BinaryMarker{Bytes: base64.StdEncoding.EncodeToString(t)}
		}
		return string(t)
	case time.Time:
		return t.Format("2006-01-02 15:04:05.999999")
	default:
		return v
	}
}

// decodeValue restores a streamed value for SQL binding.
func decodeValue(v any) (any, error) {
	if m, ok := v.(map[string]any); ok {
		if b64, ok := m["$bytes"].(string); ok && len(m) == 1 {
			return base64.StdEncoding.DecodeString(b64)
		}
		// A genuine JSON-document value: re-encode as text.
		b, err := json.Marshal(m)
		if err != nil {
			return nil, err
		}
		return string(b), nil
	}
	if s, ok := v.([]any); ok {
		b, err := json.Marshal(s)
		if err != nil {
			return nil, err
		}
		return string(b), nil
	}
	return v, nil
}

// columnTypes reports which columns of table are binary (BLOB/BINARY
// families) so values survive the JSON hop without charset coercion.
func columnTypes(ctx context.Context, q interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}, table string) (map[string]bool, error) {
	rows, err := q.QueryContext(ctx,
		`SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?`, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var col, typ string
		if err := rows.Scan(&col, &typ); err != nil {
			return nil, err
		}
		switch strings.ToLower(typ) {
		case "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob":
			out[col] = true
		}
	}
	return out, rows.Err()
}

// Checksum accumulates the canonical stream digest. Both sides feed the same
// sequence of calls and must agree on Sum.
type Checksum struct {
	h uint32
}

// NewChecksum returns an empty accumulator.
func NewChecksum() *Checksum { return &Checksum{} }

func (c *Checksum) write(s string) {
	c.h = crc32.Update(c.h, crc32.IEEETable, []byte(s))
}

// Table starts a table section.
func (c *Checksum) Table(name string) { c.write("T" + name + "\n") }

// Row feeds one row; cols must be sorted by caller... it sorts here.
func (c *Checksum) Row(id string, values map[string]any) {
	c.write("R" + id + "\n")
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		b, _ := json.Marshal(values[k])
		c.write("C" + k + "=" + string(b) + "\n")
	}
}

// Version feeds one field version.
func (c *Checksum) Version(row, col string, p, l uint64, origin string, seq uint64) {
	c.write(fmt.Sprintf("V%s/%s=%d/%d/%s/%d\n", row, col, p, l, origin, seq))
}

// Tombstone feeds one tombstone.
func (c *Checksum) Tombstone(row string, p, l uint64, origin string, seq uint64) {
	c.write(fmt.Sprintf("D%s=%d/%d/%s/%d\n", row, p, l, origin, seq))
}

// Sum returns the digest.
func (c *Checksum) Sum() uint32 { return c.h }

// Vector feeds the seed vector.
func (c *Checksum) Vector(keys []string, get func(string) uint64) {
	for _, k := range keys {
		c.write(fmt.Sprintf("O%s=%d\n", k, get(k)))
	}
}

func writeMsg(w io.Writer, namespace, kind string, body any) error {
	payload, err := protocol.Encode(namespace, kind, body)
	if err != nil {
		return err
	}
	return protocol.WriteFrame(w, payload)
}

func readEnvelope(r io.Reader, namespace string) (protocol.Envelope, error) {
	frame, err := protocol.ReadFrame(r)
	if err != nil {
		return protocol.Envelope{}, err
	}
	return protocol.Decode(frame, namespace)
}
