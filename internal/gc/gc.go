// Package gc computes per-origin garbage-collection watermarks and deletes
// safely-replicated log history in bounded batches.
//
// For every origin, the watermark is the minimum contiguous sequence across
// all ACTIVE nodes' vectors plus every active bootstrap pin. Events at or
// below the watermark exist on every node that needs them (or inside a seed
// snapshot) and can be removed. GC pauses under a stale membership epoch.
package gc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/mariamesh/mariamesh/internal/changelog"
	"github.com/mariamesh/mariamesh/internal/store"
)

// ErrStaleMembership aborts a GC cycle when the membership view is behind:
// some known node carries a newer epoch than local.
var ErrStaleMembership = errors.New("gc: membership epoch is stale, pausing collection")

// Watermarks computes the safe per-origin delete-through sequence.
func Watermarks(ctx context.Context, db *sql.DB) (changelog.Vector, error) {
	localEpoch, err := store.GetEpoch(ctx, db)
	if err != nil {
		return nil, err
	}
	nodes, err := store.ListNodes(ctx, db)
	if err != nil {
		return nil, err
	}
	for _, n := range nodes {
		if n.MembershipEpoch > localEpoch {
			return nil, fmt.Errorf("%w: node %s has epoch %d > local %d",
				ErrStaleMembership, n.NodeID, n.MembershipEpoch, localEpoch)
		}
	}
	active := map[string]bool{}
	for _, n := range nodes {
		if n.Status == store.StatusActive {
			active[n.NodeID+":"+n.IncarnationID] = true
		}
	}
	all, err := store.AllProgress(ctx, db)
	if err != nil {
		return nil, err
	}
	var vectors []changelog.Vector
	for recv, v := range all {
		if active[recv] {
			vectors = append(vectors, v)
		}
	}
	if len(vectors) == 0 {
		return changelog.Vector{}, nil // nobody to agree with: retain all
	}
	pins, err := store.ActiveBootstrapPins(ctx, db)
	if err != nil {
		return nil, err
	}
	hasSeed, err := store.HasActiveBootstrap(ctx, db)
	if err != nil {
		return nil, err
	}
	if hasSeed {
		// Each active seed is an extra receiver holding exactly its pins.
		// Origins absent from every pin row count as 0 for that receiver
		// (created after the snapshot; the joiner needs all of them), so
		// zero-fill the merged pin vector over all known origins.
		known := map[string]bool{}
		for _, v := range vectors {
			for k := range v {
				known[k] = true
			}
		}
		merged := changelog.Vector{}
		for k := range known {
			merged[k] = pins[k] // absent reads 0: retain
		}
		vectors = append(vectors, merged)
	}
	return changelog.Min(vectors...), nil
}

// Result summarizes one collection cycle.
type Result struct {
	Watermarks changelog.Vector
	Deleted    int64
	Origins    int
}

// Run deletes history at or below the watermarks in bounded batches
// (batchLimit rows per origin per cycle).
func Run(ctx context.Context, db *sql.DB, batchLimit int) (Result, error) {
	var res Result
	wm, err := Watermarks(ctx, db)
	if err != nil {
		return res, err
	}
	res.Watermarks = wm
	if batchLimit <= 0 {
		batchLimit = 1000
	}
	for _, key := range wm.Origins() {
		through := wm[key]
		if through == 0 {
			continue
		}
		o, err := changelog.ParseKey(key)
		if err != nil {
			continue
		}
		n, err := store.DeleteThrough(ctx, db, o, through, batchLimit)
		if err != nil {
			return res, err
		}
		res.Deleted += n
		res.Origins++
	}
	return res, nil
}
