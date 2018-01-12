/*
Package kitlion defines functionality to integrate lion with go-kit.

https://github.com/go-kit/kit
*/
package kitlion // import "go.pedge.io/lion/kit"

import (
	"go.pedge.io/lion"

	"github.com/go-kit/kit/log"
)

// GlobalLogger returns the global go-kit Logger.
func GlobalLogger() log.Logger {
	return NewLogger(lion.GlobalLogger())
}

// NewLogger returns a new go-kit Logger.
func NewLogger(logger lion.Logger) log.Logger {
	return newLogger(logger)
}

type logger struct {
	l lion.Logger
}

func newLogger(l lion.Logger) *logger {
	return &logger{l}
}

func (l *logger) Log(keyvals ...interface{}) error {
	l.l.WithKeyValues(keyvals...).Println()
	return nil
}
