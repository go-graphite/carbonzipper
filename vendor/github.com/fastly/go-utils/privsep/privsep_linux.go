package privsep

// This is a copy of the code removed by
// https://github.com/golang/go/commit/343b4ba8c1ad8a29b6dd19cb101273b57a26c9b0
// It is regrettably required to use the privsep package on Linux as
// of Go 1.4.  For more info, https://golang.org/issue/1435

import "syscall"

func setuid(uid int) (err error) {
	_, _, e1 := syscall.RawSyscall(syscall.SYS_SETUID, uintptr(uid), 0, 0)
	if e1 != 0 {
		err = e1
	}
	return
}

func setgid(gid int) (err error) {
	_, _, e1 := syscall.RawSyscall(syscall.SYS_SETGID, uintptr(gid), 0, 0)
	if e1 != 0 {
		err = e1
	}
	return
}
