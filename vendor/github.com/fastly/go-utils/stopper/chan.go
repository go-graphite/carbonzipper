package stopper

import (
	"sync"
)

// ChanStopper implements the Stopper interface for users that loop over a
// select block
type ChanStopper struct {
	sync.Mutex
	Chan   chan struct{}
	onDone func()
	done   bool
}

func NewChanStopper() *ChanStopper {
	return &ChanStopper{Chan: make(chan struct{}, 1)}
}

func (s *ChanStopper) Stop() {
	if s == nil {
		return
	}
	s.Lock()
	defer s.Unlock()
	close(s.Chan)
}

func (s *ChanStopper) OnDone(f func()) {
	if s == nil {
		return
	}
	s.Lock()
	defer s.Unlock()
	s.onDone = f
}

func (s *ChanStopper) Finish() {
	if s == nil {
		return
	}
	s.Lock()
	defer s.Unlock()
	if s.onDone != nil {
		s.onDone()
	}
	s.done = true
}

func (s *ChanStopper) Done() bool {
	return s.done
}
