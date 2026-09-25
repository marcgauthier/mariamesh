package quic

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"time"
)

// GenerateNodeCert creates a self-signed ECDSA certificate binding nodeID
// (a node UUID string) as a DNS SAN, plus a pool trusting it. It exists for
// tests, development, and single-CA deployments where every node shares one
// operator CA; production deployments should issue node certificates from
// their own PKI with the node ID in a SAN.
func GenerateNodeCert(nodeID string) (tls.Certificate, *x509.CertPool, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: nodeID},
		DNSNames:              []string{nodeID},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert.Leaf)
	if cert.Leaf == nil {
		leaf, err := x509.ParseCertificate(der)
		if err != nil {
			return tls.Certificate{}, nil, err
		}
		pool.AddCert(leaf)
	}
	return cert, pool, nil
}

// NodeIDFromCertificate extracts the bound node ID (first DNS SAN) from a
// peer certificate. It implements the certificate-to-node binding: after the
// handshake, the HELLO's claimed node ID must equal this value.
func NodeIDFromCertificate(cert *x509.Certificate) (string, error) {
	if cert == nil {
		return "", fmt.Errorf("quic: no peer certificate")
	}
	if len(cert.DNSNames) == 0 {
		return "", fmt.Errorf("quic: peer certificate carries no node SAN")
	}
	return cert.DNSNames[0], nil
}

// NodeIDFromPeerCerts extracts the node ID from a TLS peer chain.
func NodeIDFromPeerCerts(certs []*x509.Certificate) (string, error) {
	if len(certs) == 0 {
		return "", fmt.Errorf("quic: no peer certificates")
	}
	return NodeIDFromCertificate(certs[0])
}
