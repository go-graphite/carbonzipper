// Package privsep provides a mechanism for a privileged process to create a
// less-privileged child process with which it maintains a bidirectional
// communication channel.
package privsep

import (
	"io"
	"os"
	"strings"
)

// CreateChild forks a new process to run the program name with its args. As
// long as that program promptly calls MaybeBecomeChild, it will change its
// owner to the specified user and re-execute itself to ensure all threads have
// dropped as well.
//
// If there is a problem starting the child (e.g. the command could not be run
// or the process owner could not be changed), the child will attempt to
// communicate the error back to the parent, which will return it in err.
//
// If the operation succeeds, the returned reader and writer will be connected
// to the less-privileged child--identified by process--after it calls
// MaybeBecomeChild.
func CreateChild(username, name string, args []string, files []*os.File) (process *os.Process, r io.Reader, w io.Writer, err error) {
	return createChild(username, name, args, files)
}

// MaybeBecomeChild examines its environment to see if it was started by
// CreateChild in another process. If so, it attempts to drop privileges,
// re-execing if necessary. It should be called as early as possible in the
// life of a program that is intended to be started by CreateChild.
//
// If the process is intended to become the child, isChild will be true. If
// there is a problem becoming the child, err will be a non-nil value
// describing why. Otherwise r and w will be connected to their complements
// which were returned by CreateChild in the parent process.
//
// The same binary may be both parent and child.
func MaybeBecomeChild() (isChild bool, r io.Reader, w io.Writer, files []*os.File, err error) {
	return maybeBecomeChild()
}

// keep a copy of the original argv
var origArgs []string

func init() {
	origArgs = make([]string, len(os.Args))
	copy(origArgs, os.Args)
}

// OrigArgs returns the value of os.Args as it was set at init() time.
func OrigArgs() []string {
	args := make([]string, len(os.Args))
	copy(args, origArgs)
	return args
}

func cleanEnv() {
	// XXX replace with os.Unsetenv after
	// https://code.google.com/p/go/source/detail?r=5cf5d1f289a7
	env := os.Environ()
	os.Clearenv()
	for _, v := range env {
		if !strings.HasPrefix(v, "__privsep_") {
			if s := strings.SplitN(v, "=", 2); len(s) == 2 {
				os.Setenv(s[0], s[1])
			}
		}
	}
}

// used in tests
var envVars = []string{"__privsep_phase", "__privsep_user", "__privsep_fds"}
