// +build linux darwin

package privsep

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"runtime"
	"strconv"
	"syscall"

	"github.com/fastly/go-utils/executable"
)

const (
	exitOK               = 0
	exitOSFailure        = 1
	exitDescribedFailure = 2
	exitFailsafe         = 3

	statusOK = "ok"

	// see comment below in createChild
	inputFd      = 3
	outputFd     = 4
	statusFd     = 5
	userFdOffset = statusFd + 1
)

func createChild(username, bin string, args []string, files []*os.File) (process *os.Process, r io.Reader, w io.Writer, err error) {
	// create a pipe for each direction
	var childIn, childOut, childStatus, parentIn, parentOut, parentStatus *os.File
	childIn, parentOut, err = os.Pipe()
	if err != nil {
		return
	}
	parentIn, childOut, err = os.Pipe()
	if err != nil {
		return
	}
	parentStatus, childStatus, err = os.Pipe()
	if err != nil {
		return
	}

	child := exec.Command(bin, args...)

	// os/exec on Cmd.ExtraFiles: "If non-nil, entry i becomes file descriptor 3+i."
	// so childIn becomes fd 3 in child, childOut is 4, childStatus is 5
	child.ExtraFiles = append(child.ExtraFiles, []*os.File{childIn, childOut, childStatus}...)
	if len(files) > 0 {
		// user's files start at userFdOffset
		child.ExtraFiles = append(child.ExtraFiles, files...)
	}
	child.Stdout = os.Stdout
	child.Stderr = os.Stderr
	child.Env = append(os.Environ(), []string{
		"__privsep_phase=dropping",
		"__privsep_user=" + username,
		"__privsep_fds=" + strconv.Itoa(len(files)),
	}...)

	err = child.Start()
	if err != nil {
		err = fmt.Errorf("couldn't start child: %s", err)
		return
	}

	if status, _ := bufio.NewReader(parentStatus).ReadString('\n'); status != statusOK+"\n" {
		err = errors.New(status)
		return
	}
	parentStatus.Close()

	// parent doesn't need these anymore
	childIn.Close()
	childOut.Close()
	childStatus.Close()

	process = child.Process
	r = parentIn
	w = parentOut

	return
}

func maybeBecomeChild() (isChild bool, r io.Reader, w io.Writer, files []*os.File, err error) {

	// dropping privileges is a two-phase process since a Go program cannot
	// completely drop privileges after the runtime has started; only the
	// thread which calls setuid will have its uid changed, and there is no way
	// to iterate over all the runtime's threads to make that happen
	// process-wide. instead, the child must exec itself on the same thread
	// that calls setuid; the new runtime's threads will then all be owned by
	// the target user.

	switch os.Getenv("__privsep_phase") {

	default:
		// not the child
		return

	case "dropping":
		// phase 1: we're the child, but haven't dropped privileges

		defer os.Exit(exitFailsafe) // never return to caller from this phase

		var bin string
		bin, err = executable.Path()
		if err != nil {
			reportError(err)
		}

		// make sure the thread that exec's is the same one that drops privs
		runtime.LockOSThread()

		if err = dropPrivs(); err != nil {
			reportError(err)
		}

		const X_OK = 1 // avoid dependency on non-stdlib https://github.com/golang/sys/blob/8642817a1a1d69c31059535024a5c3f02b5b176f/unix/constants.go
		if err = syscall.Access(bin, X_OK); err != nil {
			reportError(fmt.Errorf("%s is not executable by unprivileged user (%s)", bin, err))
		}

		fds := os.Getenv("__privsep_fds")
		cleanEnv()
		os.Setenv("__privsep_phase", "dropped")
		os.Setenv("__privsep_fds", fds)

		args := append([]string{bin}, origArgs[1:]...)
		if err = syscall.Exec(bin, args, os.Environ()); err != nil {
			reportError(err)
		}

	case "dropped":
		// phase 2: we're the child, now without privileges

		isChild = true

		if os.Getuid() == 0 {
			reportError(errors.New("child is still privileged"))
		}

		nfds, _ := strconv.Atoi(os.Getenv("__privsep_fds"))

		cleanEnv()

		r = os.NewFile(inputFd, "input")
		w = os.NewFile(outputFd, "output")

		status := os.NewFile(statusFd, "status")
		fmt.Fprintln(status, statusOK)
		status.Close()

		if nfds > 0 {
			files = make([]*os.File, nfds)
			for i := 0; i < nfds; i++ {
				files[i] = os.NewFile(userFdOffset+uintptr(i), fmt.Sprintf("fd%d", i))
			}
		}
	}

	return
}

func dropPrivs() error {
	username := os.Getenv("__privsep_user")

	if username == "" {
		return errors.New("no __privsep_user")
	}

	u, err := user.Lookup(username)
	if err != nil {
		return err
	}

	uid, err := strconv.Atoi(u.Uid)
	if err != nil {
		return fmt.Errorf("uid of %s isn't numeric: %q", u.Uid, u.Uid)
	}

	gid, err := strconv.Atoi(u.Gid)
	if err != nil {
		return fmt.Errorf("gid of %s isn't numeric: %q", u.Gid, u.Gid)
	}

	groups, err := u.GroupIds()
	if err != nil {
		return err
	}

	gids := make([]int, len(groups))
	for i := range groups {
		g, err := strconv.Atoi(groups[i])
		if err != nil {
			return fmt.Errorf("gid isn't numeric: %q", groups[i])
		}
		gids[i] = g
	}

	// change groups first since they can't be changed after
	// dropping root uid
	if err := syscall.Setgroups(gids); err != nil {
		return err
	}
	if err := setgid(gid); err != nil {
		return err
	}
	if err := setuid(uid); err != nil {
		return err
	}

	return nil
}

func reportError(err error) {
	fmt.Fprintln(os.NewFile(statusFd, "status"), err.Error())
	os.Exit(exitDescribedFailure)
}
