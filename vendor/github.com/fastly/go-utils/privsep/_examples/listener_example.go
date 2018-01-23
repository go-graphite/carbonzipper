// +build none

// This example demonstrates how to open a (possibly privileged) listening port
// in the parent and run an HTTP server attached to it in an unprivileged
// child. It also shows how the parent can kill the child by its PID.

// build with `go build $GOPATH/src/github.com/fastly/go-utils/privsep/_examples/listener_example.go`

package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/fastly/go-utils/privsep"
)

func main() {
	flagUsername := flag.String("username", "nobody", "username for the unprivileged child")

	isChild, _, _, files, err := privsep.MaybeBecomeChild()
	if err != nil {
		log.Fatalf("MaybeBecomeChild failed: %s", err)
	}

	who := "parent"
	if isChild {
		who = "child"
	}
	log.Printf("%s: pid=%d uid=%d euid=%d gid=%d egid=%d",
		who, os.Getpid(), os.Getuid(), os.Geteuid(), os.Getgid(), os.Getegid())

	if isChild {
		if len(files) < 1 {
			log.Fatalf("no extra files: %v", files)
		}
		l, err := net.FileListener(files[0])
		if err != nil {
			log.Fatalf("FileListener: %s", err)
		}
		child(l)
		return
	}

	if os.Getuid() != 0 {
		log.Print("Warning: this example only works when run as the root user")
	}

	addr := "localhost:1111"
	laddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		log.Fatalf("resolve %s: %s", addr, err)
	}

	l, err := net.ListenTCP("tcp", laddr)
	if err != nil {
		log.Fatalf("listen %s: %s", laddr, err)
	}

	sock, err := l.File()
	if err != nil {
		log.Fatalf("fd: %s", err)
	}

	proc, _, _, err := privsep.CreateChild(*flagUsername, os.Args[0], nil, []*os.File{sock})
	if err != nil {
		log.Fatalf("CreateChild failed: %s", err)
	}

	sock.Close()

	// tidy up so child doesn't run forever
	defer proc.Kill()

	parent(laddr)
}

func parent(addr *net.TCPAddr) {
	url := fmt.Sprintf("http://%s/ping", addr.String())
	start := time.Now()
	for time.Since(start) < time.Second {
		res, err := http.Get(url)
		if err == nil {
			defer res.Body.Close()
			body, _ := ioutil.ReadAll(res.Body)
			log.Printf("Response for %s: %q", url, body)
			return
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	log.Fatal("No response received")
}

func child(l net.Listener) {
	log.Fatal(http.Serve(l, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "pong")
	})))
}
