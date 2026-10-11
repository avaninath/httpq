package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"
)

func TestSelfSignedCert(t *testing.T) {
	cert, err := selfSignedCert()
	if err != nil {
		t.Fatalf("selfSignedCert: %v", err)
	}
	if len(cert.Certificate) == 0 {
		t.Fatal("no certificate in the chain")
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}

	for _, host := range []string{"localhost", "127.0.0.1", "::1"} {
		if err := leaf.VerifyHostname(host); err != nil {
			t.Errorf("VerifyHostname(%q): %v", host, err)
		}
	}
	if err := leaf.VerifyHostname("example.com"); err == nil {
		t.Errorf("VerifyHostname(%q) = nil, want an error", "example.com")
	}

	now := time.Now()
	if now.Before(leaf.NotBefore) || now.After(leaf.NotAfter) {
		t.Errorf("certificate not valid now: valid from %v to %v", leaf.NotBefore, leaf.NotAfter)
	}
	if !slices.Contains(leaf.ExtKeyUsage, x509.ExtKeyUsageServerAuth) {
		t.Errorf("ExtKeyUsage = %v, want it to include server auth", leaf.ExtKeyUsage)
	}
}

func TestTLSHandshake(t *testing.T) {
	cert, err := selfSignedCert()
	if err != nil {
		t.Fatalf("selfSignedCert: %v", err)
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	srv.Config.ErrorLog = log.New(io.Discard, "", 0)
	srv.StartTLS()
	t.Cleanup(srv.Close)

	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatalf("client that trusts the certificate: %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Errorf("close body: %v", err)
	}

	_, err = http.Get(srv.URL)
	if _, ok := errors.AsType[*tls.CertificateVerificationError](err); !ok {
		t.Errorf("client that doesn't trust the certificate: err = %v, want a certificate verification error", err)
	}
}
