package transport

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"
)

const (
	tlsIdentitySalt   = "shepherd/tls-identity/v1"
	tlsCertificateTTL = 365 * 24 * time.Hour
)

var (
	errEmptySecret = errors.New("transport: empty pre-auth token for TLS")
)

func deriveIdentity(token string) string {
	sum := sha256Bytes([]byte(tlsIdentitySalt + token))
	encoded := hex.EncodeToString(sum)
	return "shepherd-" + encoded[:32]
}

func sha256Bytes(input []byte) []byte {
	h := sha256.Sum256(input)
	return h[:]
}

func deriveKeyPair(token string) (ed25519.PrivateKey, ed25519.PublicKey, error) {
	if token == "" {
		return nil, nil, errEmptySecret
	}
	seedMaterial := sha256Bytes([]byte(token + tlsIdentitySalt))
	privateKey := ed25519.NewKeyFromSeed(seedMaterial)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	return privateKey, publicKey, nil
}

func selfSignedCertificate(token, serverName string) (tls.Certificate, []byte, string, error) {
	privateKey, publicKey, err := deriveKeyPair(token)
	if err != nil {
		return tls.Certificate{}, nil, "", err
	}

	identity := deriveIdentity(token)
	dnsNames := []string{identity}
	if serverName != "" {
		if !strings.EqualFold(serverName, identity) {
			dnsNames = append(dnsNames, serverName)
		}
	}

	serial := big.NewInt(0).SetBytes(sha256Bytes([]byte(identity)))

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: identity,
		},
		NotBefore:             time.Now().Add(-5 * time.Minute),
		NotAfter:              time.Now().Add(tlsCertificateTTL),
		DNSNames:              dnsNames,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
			x509.ExtKeyUsageClientAuth,
		},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, publicKey, privateKey)
	if err != nil {
		return tls.Certificate{}, nil, "", fmt.Errorf("transport: create certificate: %w", err)
	}

	keyBytes, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return tls.Certificate{}, nil, "", fmt.Errorf("transport: marshal private key: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes})

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return tls.Certificate{}, nil, "", fmt.Errorf("transport: load key pair: %w", err)
	}

	return tlsCert, certDER, identity, nil
}

// NewServerTLSConfig 基于共享的预认证 token 构建确定性的 TLS 配置。
func NewServerTLSConfig(token, serverName string) (*tls.Config, error) {
	cert, _, _, err := selfSignedCertificate(token, serverName)
	if err != nil {
		return nil, err
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
		ClientAuth:   tls.NoClientCert,
	}, nil
}

// NewClientTLSConfig 返回绑定确定性证书的客户端 TLS 配置。
func NewClientTLSConfig(serverName, token string) (*tls.Config, error) {
	_, certDER, identity, err := selfSignedCertificate(token, serverName)
	if err != nil {
		return nil, err
	}

	rootPool := x509.NewCertPool()
	rootPool.AppendCertsFromPEM(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}))

	cfg := &tls.Config{
		RootCAs:    rootPool,
		MinVersion: tls.VersionTLS13,
	}

	if serverName != "" {
		cfg.ServerName = serverName
	} else {
		cfg.ServerName = identity
	}

	// 确保会话票据在受控节点间保持可用。
	cfg.ClientSessionCache = tls.NewLRUClientSessionCache(32)

	return cfg, nil
}

// CertificateFingerprint 计算证书的 SHA256 指纹（十六进制格式）。
func CertificateFingerprint(certDER []byte) string {
	sum := sha256.Sum256(certDER)
	return hex.EncodeToString(sum[:])
}

// GetCertificatePEM 基于 token 导出 CA 证书的 PEM 格式。
// 可用于导出给客户端进行证书校验。
func GetCertificatePEM(token, serverName string) ([]byte, string, error) {
	_, certDER, _, err := selfSignedCertificate(token, serverName)
	if err != nil {
		return nil, "", err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	fingerprint := CertificateFingerprint(certDER)

	return certPEM, fingerprint, nil
}
