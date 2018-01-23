package lifecycle

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/fastly/go-utils/executable"
	"github.com/fastly/go-utils/instrumentation"
	"github.com/fastly/go-utils/stopper"
	"github.com/fastly/go-utils/vlog"
)

const traceSignal = syscall.SIGUSR1

// A Lifecycle manages some boilerplate for running daemons.
type Lifecycle struct {
	m         sync.Mutex
	interrupt chan os.Signal
	fatalQuit chan struct{}
	killFuncs []func()
}

// New creates a new Lifecycle. This should be called after validating
// parameters but before starting work or allocating external resources. A
// startup message is displayed and shutdown handlers for SIGINT and SIGTERM
// are registered.
//
// If New is passed 'true' for singleProcess, it will wait for existing duplicate
// processes to exit before returning.
func New(singleProcess bool) *Lifecycle {
	l := Lifecycle{
		interrupt: make(chan os.Signal, 1),
		fatalQuit: make(chan struct{}, 1),
	}

	// make sigint trigger a clean shutdown
	signal.Notify(l.interrupt, os.Interrupt)
	signal.Notify(l.interrupt, syscall.SIGTERM)
	signal.Notify(l.interrupt, syscall.SIGHUP)

	if singleProcess && executable.NowRunning() {
		vlog.VLogf("Waiting for existing %s processes to exit...", os.Args[0])
		for executable.NowRunning() {
			select {
			case <-l.interrupt:
				log.Fatalf("Aborting")
			case <-time.After(100 * time.Millisecond):
			}
		}
	}

	return &l
}

// RunWhenKilled blocks until a shutdown signal is received, then executes
// finalizer and only returns either after it has finished or another
// shutdown signal is received. If timeout is non-zero, RunWhenKilled will
// force shutdown if the finalizer cannot complete within the timeout duration.
//
// RunWhenKilled should only be called once with a master function to run
// on program shutdown.
//
// RunWhenKilled runs the finalizer before any deferred AddKillFunc functions.
// This is so that the finalizer can begin the shutdown process that any
// other AddKillFunc functions can rely on.
func (l *Lifecycle) RunWhenKilled(finalizer func(), timeout time.Duration) {
	vlog.VLogf("%s started", os.Args[0])
	select {
	case sig := <-l.interrupt:
		vlog.VLogf("Caught signal %q, shutting down", sig)
	case <-l.fatalQuit:
		vlog.VLogf("Caught fatal quit, shutting down")
	}

	// wait for either confirmation that we finished or another interrupt
	shutdown := make(chan struct{}, 1)
	go func() {
		if finalizer != nil {
			finalizer()
		}
		for i := len(l.killFuncs) - 1; i >= 0; i-- {
			l.killFuncs[i]()
		}
		close(shutdown)
	}()
	var t <-chan time.Time
	if timeout > 0 {
		t = time.After(timeout)
	}
	select {
	case <-shutdown:
		vlog.VLogf("Shutdown complete, goodbye")
		os.Exit(0)
	case <-t:
		vlog.VLogf("Shutdown timeout exceeded (%v)", timeout)
		os.Exit(1)
	case <-l.interrupt:
		vlog.VLogf("Second interrupt, exiting")
		os.Exit(1)
	}
}

// AddKillFunc will add f to the list of functions to be ran
// when the lifecycle is killed. Functions passed to AddKillFunc
// are ran in reverse order, much like defer. If the lifecycle
// is being killed ad the same time AddKillFunc is called, the
// passed function will not be called.
func (l *Lifecycle) AddKillFunc(f func()) {
	l.m.Lock()
	defer l.m.Unlock()
	l.killFuncs = append(l.killFuncs, f)
}

// FatalQuit will kill the lifecycle to continue into the RunWhenKilled function.
func (l *Lifecycle) FatalQuit() {
	l.fatalQuit <- struct{}{}
}

// for debugging, show goroutine trace on receipt of USR1. uninstall by calling
// Stop on the returned object
func InstallStackTracer() stopper.Stopper {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, traceSignal)
	stopper := stopper.NewChanStopper()
	go func() {
		defer func() {
			signal.Stop(signals)
			close(signals)
		}()
		for {
			select {
			case <-signals:
				log.Print(instrumentation.GetStackTrace(true))
			case <-stopper.Chan:
				return
			}
		}
	}()
	return stopper
}

func GetStackTrace(all bool) string {
	return instrumentation.GetStackTrace(all)
}
