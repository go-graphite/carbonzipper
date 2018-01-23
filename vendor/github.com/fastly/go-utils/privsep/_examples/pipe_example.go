// +build none

// This example demonstrates how to use a pair of pipes to communicate between
// a privileged parent and unprivileged child.

// build with `go build $GOPATH/src/github.com/fastly/go-utils/privsep/_examples/pipe_example.go`

package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/fastly/go-utils/privsep"
)

func main() {
	flagUsername := flag.String("username", "nobody", "username for the unprivileged child")

	isChild, r, w, _, err := privsep.MaybeBecomeChild()
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
		child(r, w)
		return
	}

	if os.Getuid() != 0 {
		log.Print("Warning: this example only works when run as the root user")
	}

	_, r, w, err = privsep.CreateChild(*flagUsername, os.Args[0], nil, nil)
	if err != nil {
		log.Fatalf("CreateChild failed: %s", err)
	}
	parent(r, w)
}

func parent(r io.Reader, w io.Writer) {
	br := bufio.NewReader(r)
	for {
		msg := fmt.Sprintf("ping %s\n", time.Now())
		if _, err := io.WriteString(w, msg); err != nil {
			log.Fatalf("failed to write in parent: %s", err)
		}
		log.Printf("parent sent %q\n", msg)

		reply, err := br.ReadString('\n')
		if err != nil {
			log.Fatalf("failed to read in parent: %s", err)
		}
		log.Printf("parent got %q\n", reply)

		time.Sleep(time.Second)
	}
}

func child(r io.Reader, w io.Writer) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		log.Printf("child got %q", line)

		reply := fmt.Sprintf("pong %s\n", time.Now())
		if _, err := io.WriteString(w, reply); err != nil {
			log.Printf("error to write in child: %s", err)
		}
		log.Printf("child sent %q", reply)
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("failed to read in child: %s", err)
	}
}
