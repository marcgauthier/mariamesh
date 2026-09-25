// Package sync runs delta synchronization over bidirectional QUIC streams.
//
// A session is fully symmetric after the HELLO exchange (itself a port of
// Corrosion's SyncStart/State handshake): both sides send Need, both stream
// Batches, both apply-then-ACK. Either side may have initiated the stream;
// the message kinds drive a single state machine, so halves cannot deadlock.
package sync

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/mariamesh/mariamesh/internal/apply"
	"github.com/mariamesh/mariamesh/internal/changelog"
	"github.com/mariamesh/mariamesh/internal/clock"
	"github.com/mariamesh/mariamesh/internal/protocol"
	"github.com/mariamesh/mariamesh/internal/store"
)

// Logger is a minimal structured logger.
type Logger interface {
	Debug(msg string, kv ...any)
	Info(msg string, kv ...any)
	Warn(msg string, kv ...any)
	Error(msg string, kv ...any)
}

// Env wires a session to local state.
type Env struct {
	Store           *store.Store
	Applier         *apply.Applier
	Clock           *clock.Clock
	Self            changelog.Origin
	Namespace       string
	AdvertiseAddr   string
	ProtocolVersion int
	SchemaVersion   uint64
	Epoch           func() uint64
	BatchMaxEvents  int
	BatchMaxBytes   int
	// RecordPeerVector persists what the peer has (for GC).
	RecordPeerVector func(ctx context.Context, peer changelog.Origin, v changelog.Vector) error
	// CheckPeer validates the HELLO sender (retirement, schema).
	CheckPeer func(ctx context.Context, h protocol.Hello) error
	Log       Logger
}

var (
	// ErrSchemaMismatch aborts sessions across schema versions.
	ErrSchemaMismatch = errors.New("sync: schema version mismatch")
	// ErrProtocolMismatch aborts sessions across protocol versions.
	ErrProtocolMismatch = errors.New("sync: protocol version mismatch")
	// ErrPeerRetired aborts sessions with retired incarnations.
	ErrPeerRetired = errors.New("sync: peer incarnation is retired")
)

// Session runs one symmetric sync session over rw. When initiated is true
// this side sends its HELLO first, otherwise it reads first.
func Session(ctx context.Context, rw io.ReadWriter, env *Env, initiated bool) error {
	log := env.Log
	if log == nil {
		log = noopLogger{}
	}
	localVec, err := env.Store.LocalVector(ctx, env.Self)
	if err != nil {
		return err
	}
	hello := protocol.Hello{
		Protocol:        env.ProtocolVersion,
		NodeID:          env.Self.NodeID,
		IncarnationID:   env.Self.IncarnationID,
		SchemaVersion:   env.SchemaVersion,
		MembershipEpoch: env.Epoch(),
		Mode:            "store_and_forward",
		Addr:            env.AdvertiseAddr,
		Vector:          localVec,
	}
	var peer protocol.Hello
	if initiated {
		if err := writeMsg(rw, env.Namespace, protocol.KindHello, hello); err != nil {
			return err
		}
		peer, err = readHello(ctx, rw, env.Namespace)
		if err != nil {
			return err
		}
	} else {
		peer, err = readHello(ctx, rw, env.Namespace)
		if err != nil {
			return err
		}
		if err := writeMsg(rw, env.Namespace, protocol.KindHello, hello); err != nil {
			return err
		}
	}
	peerOrigin := changelog.Origin{NodeID: peer.NodeID, IncarnationID: peer.IncarnationID}
	if err := validateHello(env, peer); err != nil {
		_ = writeMsg(rw, env.Namespace, protocol.KindError, protocol.Error{Code: codeOf(err), Message: err.Error()})
		return err
	}
	if env.CheckPeer != nil {
		if err := env.CheckPeer(ctx, peer); err != nil {
			_ = writeMsg(rw, env.Namespace, protocol.KindError, protocol.Error{Code: codeOf(err), Message: err.Error()})
			return err
		}
	}

	// Both sides send Need immediately, then drive the symmetric loop.
	ranges := missingRanges(localVec, peer.Vector)
	if err := writeMsg(rw, env.Namespace, protocol.KindNeed, protocol.Need{Ranges: ranges}); err != nil {
		return err
	}

	var pending []changelog.Event
	gotComplete := false
	gotAck := false
	sentBatches := false
	for !(gotComplete && gotAck && sentBatches) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		frame, err := protocol.ReadFrame(rw)
		if err != nil {
			return fmt.Errorf("sync: read frame: %w", err)
		}
		env2, err := protocol.Decode(frame, env.Namespace)
		if err != nil {
			return err
		}
		switch env2.Kind {
		case protocol.KindNeed:
			var need protocol.Need
			if err := protocol.DecodeBody(env2, &need); err != nil {
				return err
			}
			if err := sendRanges(ctx, rw, env, need.Ranges); err != nil {
				var herr *historyError
				if errors.As(err, &herr) {
					_ = writeMsg(rw, env.Namespace, protocol.KindError, protocol.Error{Code: protocol.ErrCodeHistoryTruncated, Message: herr.Error()})
					return herr
				}
				return err
			}
			sentBatches = true
		case protocol.KindBatch:
			var b protocol.Batch
			if err := protocol.DecodeBody(env2, &b); err != nil {
				return err
			}
			pending = append(pending, b.Events...)
			if b.Complete {
				res, err := env.Applier.ApplyBatch(ctx, pending)
				if err != nil {
					return fmt.Errorf("sync: apply: %w", err)
				}
				pending = nil
				gotComplete = true
				// Commit-before-ACK: the batch transaction committed
				// inside ApplyBatch; only now advertise it.
				if err := writeMsg(rw, env.Namespace, protocol.KindAck, protocol.Ack{Vector: res.Vector}); err != nil {
					return err
				}
			}
		case protocol.KindAck:
			var ack protocol.Ack
			if err := protocol.DecodeBody(env2, &ack); err != nil {
				return err
			}
			gotAck = true
			if env.RecordPeerVector != nil {
				if err := env.RecordPeerVector(ctx, peerOrigin, ack.Vector); err != nil {
					log.Warn("sync: record peer vector failed", "err", err)
				}
			}
		case protocol.KindError:
			var perr protocol.Error
			_ = protocol.DecodeBody(env2, &perr)
			return fmt.Errorf("sync: peer error %s: %s", perr.Code, perr.Message)
		default:
			return fmt.Errorf("sync: unexpected message %q", env2.Kind)
		}
	}
	return nil
}

// historyError signals the peer needs a seed instead of deltas.
type historyError struct{ msg string }

func (e *historyError) Error() string { return e.msg }

// missingRanges builds Need ranges for what peer has beyond local.
func missingRanges(local, peer changelog.Vector) []protocol.RangeReq {
	var out []protocol.RangeReq
	for _, r := range changelog.Missing(local, peer) {
		out = append(out, protocol.RangeReq{Origin: r.Origin, From: r.From, To: r.To})
	}
	return out
}

// sendRanges streams the requested intervals in bounded batches, always
// terminating with a Complete batch (possibly empty, possibly redundant-free:
// no trailing batch follows an already-complete one).
func sendRanges(ctx context.Context, w io.Writer, env *Env, ranges []protocol.RangeReq) error {
	db := env.Store.DB()
	terminated := false
	for _, r := range ranges {
		if r.From < 1 || r.To < r.From {
			continue
		}
		from := r.From
		for from <= r.To {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			events, err := store.FetchRange(ctx, db, r.Origin, from, r.To, env.BatchMaxEvents, env.BatchMaxBytes)
			if err != nil {
				if errors.Is(err, store.ErrHistoryTruncated) {
					return &historyError{msg: err.Error()}
				}
				return err
			}
			if len(events) == 0 {
				terminated = false // gap: remaining seqs not yet present locally
				break
			}
			last := events[len(events)-1].OriginSeq
			complete := last >= r.To
			if err := writeMsg(w, env.Namespace, protocol.KindBatch, protocol.Batch{Events: events, Complete: complete}); err != nil {
				return err
			}
			terminated = complete
			if last < from {
				break // defensive: no progress
			}
			from = last + 1
			if complete {
				break
			}
		}
	}
	if !terminated {
		// Nothing was sent, or the last batch did not cover its range end
		// (local gap): terminate explicitly so the peer can apply + ACK.
		return writeMsg(w, env.Namespace, protocol.KindBatch, protocol.Batch{Complete: true})
	}
	return nil
}

func validateHello(env *Env, peer protocol.Hello) error {
	if peer.Protocol != env.ProtocolVersion {
		return fmt.Errorf("%w: peer=%d local=%d", ErrProtocolMismatch, peer.Protocol, env.ProtocolVersion)
	}
	if peer.SchemaVersion != env.SchemaVersion {
		return fmt.Errorf("%w: peer=%d local=%d", ErrSchemaMismatch, peer.SchemaVersion, env.SchemaVersion)
	}
	if peer.NodeID == "" || peer.IncarnationID == "" {
		return fmt.Errorf("sync: peer hello missing identity")
	}
	if peer.NodeID == env.Self.NodeID && peer.IncarnationID == env.Self.IncarnationID {
		return fmt.Errorf("sync: refusing self-connection")
	}
	return nil
}

func codeOf(err error) string {
	switch {
	case errors.Is(err, ErrSchemaMismatch):
		return protocol.ErrCodeSchemaMismatch
	case errors.Is(err, ErrProtocolMismatch):
		return protocol.ErrCodeProtocolMismatch
	case errors.Is(err, ErrPeerRetired):
		return protocol.ErrCodeRetired
	default:
		return protocol.ErrCodeInternal
	}
}

func readHello(ctx context.Context, r io.Reader, namespace string) (protocol.Hello, error) {
	type result struct {
		frame []byte
		err   error
	}
	ch := make(chan result, 1)
	go func() {
		frame, err := protocol.ReadFrame(r)
		ch <- result{frame, err}
	}()
	select {
	case <-ctx.Done():
		return protocol.Hello{}, ctx.Err()
	case res := <-ch:
		if res.err != nil {
			return protocol.Hello{}, res.err
		}
		e, err := protocol.Decode(res.frame, namespace)
		if err != nil {
			return protocol.Hello{}, err
		}
		if e.Kind == protocol.KindError {
			var perr protocol.Error
			_ = protocol.DecodeBody(e, &perr)
			return protocol.Hello{}, fmt.Errorf("sync: peer error %s: %s", perr.Code, perr.Message)
		}
		if e.Kind != protocol.KindHello {
			return protocol.Hello{}, fmt.Errorf("sync: expected hello, got %q", e.Kind)
		}
		var h protocol.Hello
		if err := protocol.DecodeBody(e, &h); err != nil {
			return protocol.Hello{}, err
		}
		return h, nil
	}
}

func writeMsg(w io.Writer, namespace, kind string, body any) error {
	payload, err := protocol.Encode(namespace, kind, body)
	if err != nil {
		return err
	}
	return protocol.WriteFrame(w, payload)
}

// HandleBroadcast reads broadcast frames until EOF and applies them.
// Broadcasts are unacknowledged; periodic sync repairs any loss.
func HandleBroadcast(ctx context.Context, r io.Reader, env *Env) error {
	for {
		frame, err := protocol.ReadFrame(r)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
		e, err := protocol.Decode(frame, env.Namespace)
		if err != nil {
			return err
		}
		if e.Kind != protocol.KindBroadcast {
			return fmt.Errorf("sync: unexpected uni message %q", e.Kind)
		}
		var b protocol.Broadcast
		if err := protocol.DecodeBody(e, &b); err != nil {
			return err
		}
		if len(b.Events) == 0 {
			continue
		}
		if _, err := env.Applier.ApplyBatch(ctx, b.Events); err != nil {
			return fmt.Errorf("sync: apply broadcast: %w", err)
		}
		if env.Log != nil {
			env.Log.Debug("sync: applied broadcast", "events", len(b.Events))
		}
	}
}

type noopLogger struct{}

func (noopLogger) Debug(string, ...any) {}
func (noopLogger) Info(string, ...any)  {}
func (noopLogger) Warn(string, ...any)  {}
func (noopLogger) Error(string, ...any) {}
