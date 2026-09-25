package store

import (
	"context"
	"database/sql"

	"github.com/mariamesh/mariamesh/internal/changelog"
)

// GetProgress returns the replication vector that receiver has acknowledged:
// per-origin contiguous sequences.
func GetProgress(ctx context.Context, q qtx, receiver changelog.Origin) (changelog.Vector, error) {
	rows, err := q.QueryContext(ctx,
		"SELECT `origin_node_id`, `origin_incarnation_id`, `contiguous_seq` FROM `replication_progress` WHERE `receiver_node_id` = ? AND `receiver_incarnation_id` = ?",
		receiver.NodeID, receiver.IncarnationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := changelog.Vector{}
	for rows.Next() {
		var node, inc string
		var seq uint64
		if err := rows.Scan(&node, &inc, &seq); err != nil {
			return nil, err
		}
		out[changelog.Origin{NodeID: node, IncarnationID: inc}.Key()] = seq
	}
	return out, rows.Err()
}

// SetProgress moves receiver's cursor for one origin forward (never back).
func SetProgress(ctx context.Context, q qtx, receiver, origin changelog.Origin, seq uint64) error {
	_, err := q.ExecContext(ctx,
		`INSERT INTO `+"`replication_progress`"+` (`+"`receiver_node_id`"+`, `+"`receiver_incarnation_id`"+`,
		  `+"`origin_node_id`"+`, `+"`origin_incarnation_id`"+`, `+"`contiguous_seq`"+`)
		 VALUES (?, ?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE `+"`contiguous_seq`"+` = GREATEST(`+"`contiguous_seq`"+`, VALUES(`+"`contiguous_seq`"+`))`,
		receiver.NodeID, receiver.IncarnationID, origin.NodeID, origin.IncarnationID, seq)
	return err
}

// SetProgressMany moves several cursors in one statement.
func SetProgressMany(ctx context.Context, q qtx, receiver changelog.Origin, v changelog.Vector) error {
	for _, key := range v.Origins() {
		o, err := changelog.ParseKey(key)
		if err != nil {
			continue
		}
		if err := SetProgress(ctx, q, receiver, o, v[key]); err != nil {
			return err
		}
	}
	return nil
}

// AllProgress returns every receiver's vector (for GC computation).
// Keys are receiver origin keys.
func AllProgress(ctx context.Context, q qtx) (map[string]changelog.Vector, error) {
	rows, err := q.QueryContext(ctx,
		"SELECT `receiver_node_id`, `receiver_incarnation_id`, `origin_node_id`, `origin_incarnation_id`, `contiguous_seq` FROM `replication_progress`")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]changelog.Vector{}
	for rows.Next() {
		var rNode, rInc, oNode, oInc string
		var seq uint64
		if err := rows.Scan(&rNode, &rInc, &oNode, &oInc, &seq); err != nil {
			return nil, err
		}
		rk := changelog.Origin{NodeID: rNode, IncarnationID: rInc}.Key()
		v := out[rk]
		if v == nil {
			v = changelog.Vector{}
			out[rk] = v
		}
		v[changelog.Origin{NodeID: oNode, IncarnationID: oInc}.Key()] = seq
	}
	return out, rows.Err()
}

// LocalVector returns this node's advertised vector: current_seq for the
// local origin plus stored progress for every remote origin.
func (s *Store) LocalVector(ctx context.Context, self changelog.Origin) (changelog.Vector, error) {
	return LocalVectorTx(ctx, s.db, self)
}

// LocalVectorTx is LocalVector over an explicit querier (snapshot reads).
func LocalVectorTx(ctx context.Context, q qtx, self changelog.Origin) (changelog.Vector, error) {
	v, err := GetProgress(ctx, q, self)
	if err != nil {
		return nil, err
	}
	var seq uint64
	if err := q.QueryRowContext(ctx, "SELECT `current_seq` FROM `replication_local_state` LIMIT 1").Scan(&seq); err != nil {
		if err == sql.ErrNoRows {
			seq = 0
		} else {
			return nil, err
		}
	}
	v[self.Key()] = seq
	return v, nil
}

// AdvanceContiguous recomputes receiver's contiguous cursor for one origin
// by scanning the log forward from the current cursor for the first gap.
// It persists and returns the new cursor.
func AdvanceContiguous(ctx context.Context, q qtx, receiver, origin changelog.Origin) (uint64, error) {
	var cur uint64
	err := q.QueryRowContext(ctx,
		"SELECT `contiguous_seq` FROM `replication_progress` WHERE `receiver_node_id` = ? AND `receiver_incarnation_id` = ? AND `origin_node_id` = ? AND `origin_incarnation_id` = ?",
		receiver.NodeID, receiver.IncarnationID, origin.NodeID, origin.IncarnationID).Scan(&cur)
	if err == sql.ErrNoRows {
		cur = 0
	} else if err != nil {
		return 0, err
	}
	// If the origin is the receiver itself, the cursor is current_seq:
	// local history is gap-free by construction.
	if origin == receiver {
		var seq uint64
		if err := q.QueryRowContext(ctx, "SELECT `current_seq` FROM `replication_local_state` LIMIT 1").Scan(&seq); err != nil {
			return 0, err
		}
		if seq > cur {
			if err := SetProgress(ctx, q, receiver, origin, seq); err != nil {
				return 0, err
			}
		}
		return seq, nil
	}
	const window = 4096
	for {
		rows, err := q.QueryContext(ctx,
			"SELECT `origin_seq` FROM `replication_log` WHERE `origin_node_id` = ? AND `origin_incarnation_id` = ? AND `origin_seq` > ? ORDER BY `origin_seq` ASC LIMIT ?",
			origin.NodeID, origin.IncarnationID, cur, window)
		if err != nil {
			return 0, err
		}
		next := cur + 1
		count := 0
		gap := false
		for rows.Next() {
			var seq uint64
			if err := rows.Scan(&seq); err != nil {
				rows.Close()
				return 0, err
			}
			count++
			if seq != next {
				gap = true
				break
			}
			next++
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return 0, err
		}
		cur = next - 1
		if gap || count < window {
			break
		}
	}
	if err := SetProgress(ctx, q, receiver, origin, cur); err != nil {
		return 0, err
	}
	return cur, nil
}
