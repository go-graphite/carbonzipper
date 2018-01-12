package executable

// +build darwin

// #import <mach-o/dyld.h>
// #import <libproc.h>
import "C"

import (
	"os"
	"path/filepath"
	"unsafe"
)

// documentation in executable_linux.go

func Path() (string, error) {
	var buflen C.uint32_t = 1024
	buf := make([]C.char, buflen)

	ret := C._NSGetExecutablePath(&buf[0], &buflen)
	if ret == -1 {
		buf = make([]C.char, buflen)
		C._NSGetExecutablePath(&buf[0], &buflen)
	}
	return C.GoString(&buf[0]), nil
}

// procTable returns a map of pid to binary path. see
// http://stackoverflow.com/questions/3018054/retrieve-names-of-running-processes
func procTable() map[int]string {
	n := C.proc_listpids(C.PROC_ALL_PIDS, 0, nil, 0)
	pids := make([]C.int, n)
	C.proc_listpids(C.PROC_ALL_PIDS, 0, unsafe.Pointer(&pids[0]), n)

	m := make(map[int]string, len(pids))
	var pathBuf [C.PROC_PIDPATHINFO_MAXSIZE]C.char
	for _, pid := range pids {
		if pid == 0 {
			continue
		}
		C.proc_pidpath(pid, unsafe.Pointer(&pathBuf[0]), C.PROC_PIDPATHINFO_MAXSIZE)
		m[int(pid)] = C.GoString(&pathBuf[0])
	}
	return m
}

func BinaryDuplicateProcessIDs(binary string) (pids []int, err error) {
	bin := filepath.Clean(binary)
	for pid, path := range procTable() {
		if pid != os.Getpid() && filepath.Clean(path) == bin {
			pids = append(pids, pid)
		}
	}
	return pids, nil
}
