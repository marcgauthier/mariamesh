package quic

import (
	"crypto/tls"
	"time"

	quicgo "github.com/quic-go/quic-go"
)

// EnsureALPN clones base and sets the replication ALPN when NextProtos is
// empty. QUIC requires an ALPN to complete the handshake.
func EnsureALPN(base *tls.Config, alpn string) *tls.Config {
	c := base.Clone()
	if len(c.NextProtos) == 0 {
		c.NextProtos = []string{alpn}
	}
	return c
}

// ClientTLSForPeer clones base for dialing one peer: ALPN ensured and
// ServerName pinned to the peer's node ID so hostname verification binds
// the certificate to the expected node.
func ClientTLSForPeer(base *tls.Config, alpn, peerNodeID string) *tls.Config {
	c := EnsureALPN(base, alpn)
	if peerNodeID != "" {
		c.ServerName = peerNodeID
	}
	return c
}

// DefaultQUICConfig mirrors Corrosion's QUIC transport tuning: a bounded
// idle timeout, client keepalives at half the idle timeout, 32 concurrent
// bidirectional streams (sync sessions), 256 unidirectional streams
// (broadcasts), and datagrams enabled (membership).
func DefaultQUICConfig(idleTimeout time.Duration) *quicgo.Config {
	if idleTimeout <= 0 {
		idleTimeout = 30 * time.Second
	}
	return &quicgo.Config{
		MaxIdleTimeout:        idleTimeout,
		KeepAlivePeriod:       idleTimeout / 2,
		MaxIncomingStreams:    32,
		MaxIncomingUniStreams: 256,
		EnableDatagrams:       true,
	}
}
