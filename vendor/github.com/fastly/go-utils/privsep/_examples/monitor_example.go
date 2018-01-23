// +build none

// This example demonstrates how to monitor the exiting of child processes in
// the parent.

// build with `go build $GOPATH/src/github.com/fastly/go-utils/privsep/_examples/monitor_example.go`

package main

import (
	"flag"
	"log"
	"math/rand"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/fastly/go-utils/privsep"
)

func main() {
	var (
		flagUsername = flag.String("username", "nobody", "username for the unprivileged child")
		flagChildren = flag.Int("children", 32, "exit code")
		flagPause    = flag.Duration("pause", 10*time.Second, "time for children to sleep")
		flagTimeout  = flag.Duration("timeout", 5*time.Second, "time for parent to wait")
	)

	isChild, _, _, _, err := privsep.MaybeBecomeChild()
	if err != nil {
		log.Fatalf("MaybeBecomeChild failed: %s", err)
	}

	if isChild {
		rand.Seed(int64(time.Now().Nanosecond()))
		sleep := time.Duration(rand.Float64() * float64(*flagPause))
		code := rand.Intn(8)
		log.Printf("[%d] sleep(%.1fs), exit(%d)", os.Getpid(), sleep.Seconds(), code)
		time.Sleep(sleep)
		os.Exit(code)
		return
	}

	if os.Getuid() != 0 {
		log.Fatal("Error: this example only works when run as the root user")
	}

	var procsMu sync.Mutex
	procs := make(map[interface{}]*os.Process)

	var wg sync.WaitGroup
	wg.Add(*flagChildren)
	for i := 0; i < *flagChildren; i++ {
		proc, _, _, err := privsep.CreateChild(*flagUsername, os.Args[0], nil, nil)
		if err != nil {
			log.Fatalf("CreateChild failed: %s", err)
		}

		procsMu.Lock()
		procs[proc] = proc
		procsMu.Unlock()

		go func() {
			defer wg.Done()
			status, err := proc.Wait()
			if err != nil {
				log.Printf("child %d errored: %s", proc.Pid, err)
			}

			procsMu.Lock()
			delete(procs, proc)
			procsMu.Unlock()

			// detailed status information is available by casting to the
			// appropriate type for the platform
			if s, ok := status.Sys().(syscall.WaitStatus); ok {
				log.Printf("child %d exited with code %d", proc.Pid, s.ExitStatus())
			}
		}()
	}

	go func() {
		time.Sleep(*flagTimeout)
		log.Print("Parent timed out, killing children")
		procsMu.Lock()
		defer procsMu.Unlock()
		for _, proc := range procs {
			proc.Kill()
		}
	}()

	wg.Wait()
}
