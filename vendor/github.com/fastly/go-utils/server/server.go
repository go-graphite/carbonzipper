package server

import (
	"net"
)

/*
   Server is a abstraction that uses a channel to implement synchronization
   points between the server and its creator.

   Sync point                                   Caller method         Server method
   ----------------------------------------------------------------------------
   Server is ready to accept connections        WaitForReady          SignalReady
   Server should stop accepting connections     RequestShutdown       WaitForShutdown
   Server listeners have closed                 WaitForFinish         SignalFinish

   Additionally, the Shutdown() client method is defer-friendly shorthand for
   calling RequestShutdown then WaitForFinish.
*/

var Ping Signal

type (
	Signal     struct{}
	SignalChan chan Signal
)

type Server struct {
	Listeners map[string]net.Listener
	control   SignalChan
	stopping  bool
}

// addrs is updated with the actual listener address after binding. This allows
// requesting a random unused port by omitting the port part of an Addr.
func NewServer(addrs map[string]string) (s *Server, err error) {
	s = &Server{
		Listeners: make(map[string]net.Listener),
		control:   make(SignalChan),
	}

	for label, addr := range addrs {
		var listener net.Listener
		listener, err = net.Listen("tcp", addr)
		if err != nil {
			s.closeListeners()
			s = nil
			return
		}
		s.Listeners[label] = listener
		addrs[label] = listener.Addr().String()
	}
	return
}

const SINGLE = "\x0000"

func NewSingleServer(addr *string) (s *Server, err error) {
	a := map[string]string{SINGLE: *addr}
	if s, err = NewServer(a); err != nil {
		s = nil
		return
	}
	*addr = a[SINGLE]
	return
}

func (s *Server) Listener() net.Listener {
	if _, ok := s.Listeners[SINGLE]; !ok {
		panic("must use Listener with servers created by NewSingleServer")
	}
	return s.Listeners[SINGLE]
}

func (s *Server) SetListener(listener net.Listener) {
	if s == nil {
		return
	}
	if _, ok := s.Listeners[SINGLE]; !ok {
		panic("must use SetListener with servers created by NewSingleServer")
	}
	s.Listeners[SINGLE] = listener
}

func (s *Server) WaitForReady() {
	if s == nil {
		return
	}
	<-s.control
}

func (s *Server) RequestShutdown() {
	if s == nil {
		return
	}
	s.stopping = true
	s.control <- Ping
}

func (s *Server) WaitForFinish() {
	if s == nil {
		return
	}
	<-s.control
}

func (s *Server) SignalReady() {
	if s == nil {
		return
	}
	s.control <- Ping
}

func (s *Server) WaitForShutdown() {
	if s == nil {
		return
	}
	<-s.control
	s.closeListeners()
}

func (s *Server) SignalFinish() {
	if s == nil {
		return
	}
	s.control <- Ping
}

func (s *Server) closeListeners() {
	if s == nil {
		return
	}
	for _, listener := range s.Listeners {
		if listener != nil {
			listener.Close()
		}
	}
}

func (s *Server) Shutdown() {
	if s == nil {
		return
	}
	s.RequestShutdown()
	s.WaitForFinish()
}

func (s *Server) IsStopping() bool {
	return s.stopping
}
