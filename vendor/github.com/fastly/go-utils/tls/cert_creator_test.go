package tls_test

import (
	"testing"

	gotls "crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/fastly/go-utils/tls"
)

func init() {
	log.SetFlags(log.Lmicroseconds)
}

// test that CertCreator makes client/server certs that 1) work when used
// correctly and 2) don't work when used with different CAs
func TestCertCreator(t *testing.T) {
	cc := tls.NewCertCreator()
	cc.KeySize = 512
	cc.Country = "US"
	cc.State = "CA"
	cc.City = "San Francisco"
	cc.Organization = "Fastly Testing"

	var wg sync.WaitGroup
	done := false

	type group struct {
		name         string
		clientConfig *gotls.Config
		serverConfig *gotls.Config
		listener     net.Listener
		errors       chan error
		cleanup      func()
	}

	// generate ca, client, and server keypairs with the given name, and stand
	// up a TLS listener
	setup := func(name string) (g *group, err error) {
		g = &group{
			name:   name,
			errors: make(chan error),
			cleanup: func() {
				for _, f := range []string{
					"-ca-key.pem",
					"-ca-cert.pem",
					"client-key.pem",
					"client-cert.pem",
					"server-key.pem",
					"server-cert.pem",
				} {
					file := "testcerts/" + name + f
					if err := os.Remove(file); err != nil {
						t.Errorf("couldn't remove %q: %s", err)
					}
				}
			},
		}

		// put certs in testcerts/ since LocatePackagedPEMDir looks there
		root, err := cc.GenerateRootKeyPair("testcerts/"+name+"-ca", name+" CA")
		if err != nil {
			t.Error(err)
			return
		}

		host := "0.0.0.0"

		_, err = cc.GenerateKeyPair(tls.CLIENT, root, "testcerts/"+name+"client", host, host)
		if err != nil {
			t.Error(err)
			return
		}

		_, err = cc.GenerateKeyPair(tls.SERVER, root, "testcerts/"+name+"server", host, host)
		if err != nil {
			t.Error(err)
			return
		}

		g.clientConfig, err = tls.ConfigureClient(name+"client", name+"-ca")
		if err != nil {
			t.Errorf("%s client: %s", name+"client", err)
			return
		}

		g.serverConfig, err = tls.ConfigureServer(name+"server", name+"-ca")
		if err != nil {
			t.Errorf("%s server: %s", name+"server", err)
			return
		}

		listener, err := gotls.Listen("tcp4", host+":0", g.serverConfig)
		if err != nil {
			t.Errorf("%s listen: %s", name, err)
			return
		}
		g.listener = listener
		g.clientConfig.ServerName = g.listener.Addr().(*net.TCPAddr).IP.String() // required for client to validate server cert
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				conn, err := listener.Accept()
				if done {
					return
				}
				if err != nil {
					g.errors <- err
					return
				}
				if conn == nil {
					g.errors <- fmt.Errorf("no conn or err")
					return
				}
				wg.Add(1)
				go func(conn net.Conn) {
					defer wg.Done()
					defer conn.Close()

					conn.SetDeadline(time.Now().Add(time.Second))
					err = conn.(*gotls.Conn).Handshake()
					if err != nil {
						g.errors <- err
						return
					}

					conn.SetDeadline(time.Now().Add(time.Second))
					b := make([]byte, 3) // len("foo")
					n, err := conn.Read(b)
					b = b[:n]
					if err != nil {
						g.errors <- err
						return
					}
					if string(b) != "foo" {
						g.errors <- fmt.Errorf("%q != %q", b, "foo")
						return
					}
					conn.SetDeadline(time.Now().Add(time.Second))
					n, err = io.WriteString(conn, "bar")
					if err != nil {
						g.errors <- err
						return
					}
					g.errors <- nil
				}(conn)
			}
		}()
		return
	}

	// try to create a TLS connection from client to server and send a bit of
	// data back and forth
	check := func(client, server *group, expectedClientError, expectedServerError string) {
		addr := server.listener.Addr().String()
		conn, err := net.DialTimeout("tcp4", addr, time.Second)
		if err != nil {
			t.Errorf("%s->%s: dial: %s", client.name, server.name, err)
			return
		}

		tlsConn := gotls.Client(conn, client.clientConfig)
		clientErr := tlsConn.Handshake()
		if clientErr == nil {

			state := tlsConn.ConnectionState()
			wantAlgo := x509.SHA256WithRSA
			gotAlgo := state.PeerCertificates[0].SignatureAlgorithm
			if wantAlgo != gotAlgo {
				t.Errorf("expected signature algorithm %v, got %v", wantAlgo, gotAlgo)
			}

			tlsConn.SetDeadline(time.Now().Add(time.Second))
			if _, err := io.WriteString(tlsConn, "foo"); err == nil {
				tlsConn.SetDeadline(time.Now().Add(time.Second))
				b := make([]byte, 3) // len("bar")
				n, err := tlsConn.Read(b)
				b = b[:n]
				if err != nil {
					clientErr = err
				}
				if string(b) != "bar" {
					clientErr = fmt.Errorf("%q != %q", b, "bar")
				}
			} else {
				clientErr = err
			}
			tlsConn.Close()
		}

		var serverErr error
		select {
		case serverErr = <-server.errors:
		case <-time.After(time.Second):
			t.Errorf("timed out on serverErr")
			return
		}

		if expectedClientError == "" && clientErr != nil {
			t.Errorf("%s->%s client: should've worked but saw err=`%s`", client.name, server.name, clientErr)
		}
		if expectedClientError != "" {
			if clientErr == nil {
				t.Errorf("%s->%s client: should not have worked", client.name, server.name)
			} else if clientErr.Error() != expectedClientError {
				t.Errorf("%s->%s client: expected error %q but got %q", client.name, server.name, expectedClientError, clientErr)
			}
		}
		if expectedServerError == "" && serverErr != nil {
			t.Errorf("%s->%s server: should've worked but saw err=`%s`", client.name, server.name, serverErr)
		}
		if expectedServerError != "" {
			if serverErr == nil {
				t.Errorf("%s->%s server: should not have worked", client.name, server.name)
			} else if serverErr.Error() != expectedServerError {
				t.Errorf("%s->%s server: expected error %q but got %q", client.name, server.name, expectedServerError, serverErr)
			}
		}
	}

	// test that friend client talks to friend server, but friend client does
	// not talk to foe server or foe client to friend server
	friend, err := setup("friend")
	if err != nil {
		t.Errorf("setup: %s", err)
		return
	}
	defer friend.cleanup()

	foe, err := setup("foe")
	if err != nil {
		t.Errorf("setup: %s", err)
		return
	}
	defer foe.cleanup()

	// friend to friend: should work
	check(friend, friend, "", "")

	// friend to foe: friend client should reject foe server
	check(friend, foe, "x509: certificate signed by unknown authority", "remote error: tls: bad certificate")

	// foe to friend: foe client accepts friend server but server should reject
	// foe client.

	// prevent client from rejecting the server's "bad" cert so we can test
	// that the server rejects the client's bad cert.
	foe.clientConfig.InsecureSkipVerify = true

	// this expects no certificate from the client because
	// tls/handshake_server.go sends a list of acceptable CA certs to the
	// client when requesting a client cert and tls/handshake_client.go avoids
	// sending any cert if no eligible one is in its Config.Certificates.
	//
	// a more complete test would be to force the client to send a cert it
	// knows is bad, but that's a hassle in a pure test so I verified it
	// manually with a Go 1.2.1 patched to not send the list of CAs from the
	// server and got:
	/*
			--- FAIL: TestCertCreator (0.20 seconds)
		        cert_creator_test.go:224: foe->friend server: expected error "tls: client didn't provide a certificate" but got "tls: failed to verify client's certificate: x509: certificate signed by unknown authority"
	*/
	check(foe, friend, "remote error: tls: bad certificate", "tls: client didn't provide a certificate")
	foe.clientConfig.InsecureSkipVerify = false

	done = true
	friend.listener.Close()
	foe.listener.Close()

	c := make(chan struct{})
	go func() {
		wg.Wait()
		c <- struct{}{}
	}()

	select {
	case <-c:
	case <-time.After(time.Second):
		t.Errorf("timed out on wg.Wait")
	}
}

func TestCAIntermediate(t *testing.T) {
	cc := tls.NewCertCreator()
	cc.KeySize = 512
	prefix := "testcerts/intermediatetest"

	defer func() {
		for _, f := range []string{
			"-ca-key.pem",
			"-ca-cert.pem",
			"-intermediate-key.pem",
			"-intermediate-cert.pem",
			"-server-key.pem",
			"-server-cert.pem",
		} {
			os.Remove(prefix + f)
		}
	}()

	root, err := cc.GenerateRootKeyPair(prefix+"-ca", "Test CA")
	if err != nil {
		t.Fatalf("Couldn't generate CA cert: %s", err)
	}

	intermediate, err := cc.GenerateKeyPair(tls.CA, root, prefix+"-intermediate", "Test Intermediate")
	if err != nil {
		t.Fatalf("Couldn't generate intermediate cert: %s", err)
	}

	_, err = cc.GenerateKeyPair(tls.SERVER, intermediate, prefix+"-server", "example.com", "*.example.com")
	if err != nil {
		t.Fatalf("Couldn't generate server cert: %s", err)
	}
}
