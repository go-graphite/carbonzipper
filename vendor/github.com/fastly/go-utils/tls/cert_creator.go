// Portions Copyright 2009 The Go Authors. All rights reserved. Use of this
// source code is governed by a BSD-style license that can be found in the
// LICENSE file.
//
// Portions Copyright 2014 Fastly, Inc.

package tls

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"math/big"
	"net"
	"os"
	"time"

	"github.com/fastly/go-utils/vlog"
)

// CertCreator generates client or server public/private RSA keypairs signed by
// a generated self-signed certificate authority (CA). It will reload the CA
// cert from disk if present, and will not overwrite a keypair if either the
// key or cert exists on disk, so a cert generation program can be safely
// re-run after adding new certs. All keys are granted the minimal set of
// extended key usages for their purpose.
//
// The default values described below are not for the zero value, but rather
// those returned by NewCertCreator().
//
// Example:
//
//	cc := tls.NewCertCreator()
//	root := cc.GenerateRootKeyPair("my-ca", "My CA")
//	host := "*.mydomain.com"
//	cc.GenerateKeyPair(CLIENT, root, "proxy-client", host, host)
//	cc.GenerateKeyPair(SERVER, root, "proxy-server", host, host)
type CertCreator struct {
	// Serial number, defaults to 1.
	Serial int64
	// Time period in which the cert if valid, defaults to the current time
	// until the maximum of 2049-12-13.
	NotBefore, NotAfter time.Time
	// Key size in bytes, defaults to 4096.
	KeySize int
	// Descriptive names for the organization creating the CA. Defaults to
	// empty.
	Country, State, City, Organization string
}

// NewCertCreator returns a CertCreator with default values.
func NewCertCreator() *CertCreator {
	return &CertCreator{
		Serial:    1,
		NotBefore: time.Now(),
		NotAfter:  time.Date(2049, 12, 31, 23, 59, 59, 0, time.UTC), // end of ASN.1 time
		KeySize:   4096,
	}
}

type Purpose int

const (
	// CA certs are used only for signing other certs.
	CA Purpose = iota
	// CLIENT certs are presented by clients which initiate TLS connections.
	CLIENT
	// SERVER certs are presented by servers which accept TLS connections.
	SERVER
)

type KeyPair struct {
	Cert    *x509.Certificate
	PrivKey *rsa.PrivateKey
}

// GenerateRootKeyPair creates or reloads a self-signed CA cert.
func (cc *CertCreator) GenerateRootKeyPair(name string, commonName string, hosts ...string) (*KeyPair, error) {
	return cc.GenerateKeyPair(CA, nil, name, commonName, hosts...)
}

// GenerateKeyPair generates or reloads an RSA keypair with key usages
// determined by `purpose`. The disk files that are generated or reused are
// named `name`-key.pem and `name`-cert.pem for the private and public halves,
// respectively. `commonName` and `hosts` are the corresponding fields in the
// certificate.
//
// cc.Serial is incremented for each key that is freshly generated.
func (cc *CertCreator) GenerateKeyPair(purpose Purpose, parent *KeyPair, name string, commonName string, hosts ...string) (*KeyPair, error) {
	if pair, err := LoadKeyPairFromDisk(name); err == nil {
		return pair, nil
	}

	keyFile, certFile := name+"-key.pem", name+"-cert.pem"
	if _, err := os.Stat(certFile); !os.IsNotExist(err) {
		return nil, fmt.Errorf("Cert file %q already exists", certFile)
	}

	var extUsages []x509.ExtKeyUsage
	if purpose == CA {
		extUsages = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}
	} else if purpose == SERVER {
		extUsages = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	} else {
		extUsages = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}

	serial := new(big.Int).SetInt64(cc.Serial)
	cc.Serial++
	template := x509.Certificate{
		SignatureAlgorithm: x509.SHA256WithRSA,

		Subject: pkix.Name{
			Country:      []string{cc.Country},
			Province:     []string{cc.State},
			Locality:     []string{cc.City},
			Organization: []string{cc.Organization},
			CommonName:   commonName,
			SerialNumber: serial.String(),
		},

		NotBefore:    cc.NotBefore,
		NotAfter:     cc.NotAfter,
		SerialNumber: serial,

		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           extUsages,
		BasicConstraintsValid: true, // enforces IsCA and KeyUsage
	}
	if purpose == CA {
		template.IsCA = true
		template.KeyUsage = x509.KeyUsageCertSign
	} else {
		template.MaxPathLen = 1
	}

	// generate IP and DNS SANs
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, h)
		}
	}

	var privKey *rsa.PrivateKey
	var reusedKey bool
	if keyBytes, err := ioutil.ReadFile(keyFile); err == nil {
		// reuse existing key
		keyDERBlock, _ := pem.Decode(keyBytes)
		if keyDERBlock == nil {
			return nil, errors.New("failed to parse key PEM data")
		}
		if keyDERBlock.Type != "RSA PRIVATE KEY" {
			return nil, fmt.Errorf("key is not a RSA PRIVATE KEY: %s", keyDERBlock.Type)
		}
		privKey, err = x509.ParsePKCS1PrivateKey(keyDERBlock.Bytes)
		if err != nil {
			return nil, err
		}
		reusedKey = true
		vlog.VLogf("Reusing key %q\n", keyFile)
	} else if os.IsNotExist(err) {
		// generate a new key
		privKey, err = rsa.GenerateKey(rand.Reader, cc.KeySize)
		if err != nil {
			return nil, fmt.Errorf("Failed to generate private key: %s", err)
		}
	} else {
		return nil, err
	}

	origParent := parent
	if parent == nil {
		// CA signs itself
		parent = &KeyPair{&template, privKey}
	}

	// sign the key
	derBytes, err := x509.CreateCertificate(rand.Reader, &template, parent.Cert, &privKey.PublicKey, parent.PrivKey)
	if err != nil {
		return nil, err
	}

	// check that the cert can be parsed
	cert, err := x509.ParseCertificate(derBytes)
	if err != nil {
		return nil, fmt.Errorf("Cert doesn't verify against its CA: %s", err)
	}

	// check that the cert verifies against its own CA
	roots := x509.NewCertPool()
	if origParent == nil {
		roots.AddCert(cert)
	} else {
		roots.AddCert(parent.Cert)
	}
	_, err = cert.Verify(x509.VerifyOptions{Roots: roots, KeyUsages: extUsages})
	if err != nil {
		return nil, fmt.Errorf("Couldn't verify %q against its own CA: %s", name, err)
	}

	// write key and cert to disk
	if !reusedKey {
		keyOut, err := os.OpenFile(keyFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
		if err != nil {
			return nil, fmt.Errorf("Failed to open %q for writing: %s", keyFile, err)
		}
		pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privKey)})
		keyOut.Close()
		vlog.VLogf("Wrote key %q\n", keyFile)
	}

	certOut, err := os.Create(certFile)
	if err != nil {
		return nil, fmt.Errorf("Failed to open %q for writing: %s", certFile, err)
	}
	pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	certOut.Close()
	vlog.VLogf("Wrote cert %q\n", certFile)

	return LoadKeyPairFromDisk(name)
}

// LoadKeyPairFromDisk returns a KeyPair from disk files based on the given
// name.
func LoadKeyPairFromDisk(name string) (*KeyPair, error) {
	// cert
	certFile := name + "-cert.pem"
	b, err := ioutil.ReadFile(certFile)
	if err != nil {
		return nil, err
	}

	block, _ := pem.Decode(b)
	if block == nil {
		return nil, fmt.Errorf("No PEM data found in %q", certFile)
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, err
	}

	vlog.VLogf("Loaded cert from %s", certFile)

	// private key
	keyFile := name + "-key.pem"
	b, err = ioutil.ReadFile(keyFile)
	if err != nil {
		return nil, err
	}

	block, _ = pem.Decode(b)
	if block == nil {
		return nil, fmt.Errorf("No PEM data found in %q", keyFile)
	}

	key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}

	vlog.VLogf("Loaded key from %s", keyFile)

	return &KeyPair{cert, key}, nil
}
