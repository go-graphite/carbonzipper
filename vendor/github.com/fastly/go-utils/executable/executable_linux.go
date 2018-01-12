package executable

// +build linux

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

// Path returns the executable path of the running process.
func Path() (string, error) {
	return os.Readlink("/proc/self/exe")
}

// BinaryDuplicateProcessIDs returns all pids belonging to processes with
// the same passed binary name.
func BinaryDuplicateProcessIDs(binary string) (pids []int, err error) {
	infos, err := ioutil.ReadDir("/proc/")
	if err != nil {
		return nil, fmt.Errorf("Couldn't read /proc: %s", err)
	}
	for _, info := range infos {
		// only want numeric directories
		pid, err := strconv.Atoi(info.Name())
		if err != nil || !info.IsDir() || pid == os.Getpid() {
			continue
		}

		exe, err := os.Readlink("/proc/" + info.Name() + "/exe")
		if err != nil {
			continue
		}
		if strings.HasPrefix(exe, binary) { // if the proc's binary was deleted, it'll have suffix " (deleted)"
			pids = append(pids, pid)
		}
	}
	return pids, nil
}
