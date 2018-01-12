// Package debug is used for synchronizing a debug option across multiple
// packages littered through a project.
package debug

import (
	"sync"
)

var debug bool
var m sync.Mutex

// On returns whether debugging is on or off.
func On() bool {
	return debug
}

func TurnOn() {
	m.Lock()
	defer m.Unlock()
	debug = true
}

func TurnOff() {
	m.Lock()
	defer m.Unlock()
	debug = false
}
