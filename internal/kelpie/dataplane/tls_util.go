package dataplane

import (
	"crypto/tls"
	"crypto/x509"
	"os"
)

func loadCertificate(certFile, keyFile, clientCA string) (tls.Certificate, *x509.CertPool, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	var pool *x509.CertPool
	if clientCA != "" {
		pem, err := os.ReadFile(clientCA)
		if err != nil {
			return tls.Certificate{}, nil, err
		}
		pool = x509.NewCertPool()
		pool.AppendCertsFromPEM(pem)
	}
	return cert, pool, nil
}
