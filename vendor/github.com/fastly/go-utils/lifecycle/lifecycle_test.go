package lifecycle

import (
	"testing"

	"os"
	"time"
)

func TestRunWhenKilled(t *testing.T) {
	i := 0
	finalFunc := func() {
		i++
	}
	l := New(true)
	go func() {
		l.RunWhenKilled(finalFunc, 100*time.Millisecond)
	}()
	proc, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatalf("%v", err)
	}
	proc.Signal(os.Interrupt)
	time.Sleep(time.Millisecond)
	if i != 1 {
		t.Errorf("got %v != expect 1", i)
	}
}

func TestAddKillFunc(t *testing.T) {
	i := 0
	addKillFunc := func() {
		i++
	}
	done := make(chan struct{})
	go func() {
		l := New(true)
		defer l.RunWhenKilled(func() {
			done <- struct{}{}
		}, 100*time.Millisecond)

		l.AddKillFunc(addKillFunc)
	}()
	proc, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatalf("%v", err)
	}
	proc.Signal(os.Interrupt)
	<-done
	if i != 1 {
		t.Errorf("got %v, != expect 1", i)
	}
}

func TestFatalQuit(t *testing.T) {
	i := 0
	finalFunc := func() {
		i++
	}
	done := make(chan struct{})
	go func() {
		l := New(true)
		defer l.RunWhenKilled(finalFunc, 100*time.Millisecond)
		l.AddKillFunc(func() {
			done <- struct{}{}
		})
		go func() {
			l.FatalQuit()
		}()
	}()
	proc, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatalf("%v", err)
	}
	proc.Signal(os.Interrupt)
	<-done
	if i != 1 {
		t.Errorf("got %v, != expect 1", i)
	}
}
