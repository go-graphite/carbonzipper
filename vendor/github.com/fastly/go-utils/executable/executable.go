// Package executable has functions to return the executable path
// or directory and other process testing functions.
package executable

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"syscall"

	"github.com/fastly/go-utils/vlog"
)

// NowRunning returns true if there is a running process whose
// binary has the same name as this one.
func NowRunning() bool {
	binary, err := Path()
	if err != nil {
		log.Fatalf("Couldn't find own process: %s", err)
		return false
	}
	proc, _, err := FindDuplicateProcess(binary)
	if err != nil {
		vlog.VLogf("Couldn't look for running processes: %s", err)
		return false
	}
	if proc == nil {
		return false
	}
	if proc.Signal(syscall.Signal(0x0)) == nil {
		return true
	}
	return false
}

// Dir returns the running executable process's directory.
func Dir() (dir string, err error) {
	path, err := Path()
	if err != nil {
		return
	}
	dir, _ = filepath.Split(path)
	return
}

// FindDuplicateProcess looks for any other processes with the same
// binary name as passed in and returns the first one found.
func FindDuplicateProcess(binary string) (*os.Process, int, error) {
	all, err := BinaryDuplicateProcessIDs(binary)
	if err != nil {
		return nil, 0, err
	}
	if len(all) > 0 {
		p, err := os.FindProcess(all[0])
		if err != nil {
			return nil, 0, err
		}
		return p, all[0], nil
	}
	return nil, 0, nil
}

// DuplicateProcessIDs returns all pids belonging to processes with the
// same binary name as the running program.
func DuplicateProcessIDs() (pids []int, err error) {
	binary, err := Path()
	if err != nil {
		return nil, fmt.Errorf("Can't get path: %v", err)
	}
	return BinaryDuplicateProcessIDs(binary)
}
