/*
Package thriftlion defines the Thrift functionality for lion.
*/
package thriftlion // import "go.pedge.io/lion/thrift"

import (
	"io"
	"sync"

	"git.apache.org/thrift.git/lib/go/thrift"
	"go.pedge.io/lion"
)

var (
	// Encoding is the name of the encoding.
	Encoding = "thrift-binary"

	globalLogger Logger
	globalLevel  = lion.DefaultLevel
	globalLock   = &sync.Mutex{}
)

func init() {
	if err := lion.RegisterEncoderDecoder(
		Encoding,
		newEncoderDecoder(
			Encoding,
			thrift.NewTBinaryProtocolFactory(
				true,
				true,
			),
		),
	); err != nil {
		panic(err.Error())
	}
	lion.AddGlobalHook(setGlobalLogger)
}

func setGlobalLogger(logger lion.Logger) {
	globalLock.Lock()
	defer globalLock.Unlock()
	globalLogger = NewLogger(logger)
	globalLevel = logger.Level()
}

// MustRegister calls Register and panics on error.
func MustRegister(constructor interface{}) {
	if err := Register(constructor); err != nil {
		panic(err.Error())
	}
}

// Register registers the given thrift.TStruct using the generated constructor.
//
// The given constructor must be of the correct type.
func Register(constructor interface{}) error {
	return register(constructor)
}

// LevelLogger is a lion.LevelLogger that also has proto logging methods.
type LevelLogger interface {
	lion.BaseLevelLogger

	WithField(key string, value interface{}) LevelLogger
	WithFields(fields map[string]interface{}) LevelLogger
	WithKeyValues(keyvalues ...interface{}) LevelLogger
	WithContext(context thrift.TStruct) LevelLogger

	Print(event thrift.TStruct)
}

// Logger is a lion.Logger that also has proto logging methods.
type Logger interface {
	lion.BaseLogger

	AtLevel(level lion.Level) Logger
	WithField(key string, value interface{}) Logger
	WithFields(fields map[string]interface{}) Logger
	WithKeyValues(keyValues ...interface{}) Logger

	WithContext(context thrift.TStruct) Logger
	Debug(event thrift.TStruct)
	Info(event thrift.TStruct)
	Warn(event thrift.TStruct)
	Error(event thrift.TStruct)
	Fatal(event thrift.TStruct)
	Panic(event thrift.TStruct)
	Print(event thrift.TStruct)

	// NOTE: this function name may change, this is experimental
	LogDebug() LevelLogger
	// NOTE: this function name may change, this is experimental
	LogInfo() LevelLogger

	LionLogger() lion.Logger
}

// GlobalLogger returns the global Logger instance.
func GlobalLogger() Logger {
	return globalLogger
}

// NewLogger returns a new Logger.
func NewLogger(delegate lion.Logger) Logger {
	return newLogger(delegate)
}

// Flush calls Flush on the global Logger.
func Flush() error {
	return globalLogger.Flush()
}

// DebugWriter calls DebugWriter on the global Logger.
func DebugWriter() io.Writer {
	return globalLogger.DebugWriter()
}

// InfoWriter calls InfoWriter on the global Logger.
func InfoWriter() io.Writer {
	return globalLogger.InfoWriter()
}

// WarnWriter calls WarnWriter on the global Logger.
func WarnWriter() io.Writer {
	return globalLogger.WarnWriter()
}

// ErrorWriter calls ErrorWriter on the global Logger.
func ErrorWriter() io.Writer {
	return globalLogger.ErrorWriter()
}

// Writer calls Writer on the global Logger.
func Writer() io.Writer {
	return globalLogger.Writer()
}

// Debugf calls Debugf on the global Logger.
func Debugf(format string, args ...interface{}) {
	if lion.LevelDebug < globalLevel {
		return
	}
	globalLogger.Debugf(format, args...)
}

// Debugln calls Debugln on the global Logger.
func Debugln(args ...interface{}) {
	if lion.LevelDebug < globalLevel {
		return
	}
	globalLogger.Debugln(args...)
}

// Infof calls Infof on the global Logger.
func Infof(format string, args ...interface{}) {
	if lion.LevelInfo < globalLevel {
		return
	}
	globalLogger.Infof(format, args...)
}

// Infoln calls Infoln on the global Logger.
func Infoln(args ...interface{}) {
	if lion.LevelInfo < globalLevel {
		return
	}
	globalLogger.Infoln(args...)
}

// Warnf calls Warnf on the global Logger.
func Warnf(format string, args ...interface{}) {
	if lion.LevelWarn < globalLevel {
		return
	}
	globalLogger.Warnf(format, args...)
}

// Warnln calls Warnln on the global Logger.
func Warnln(args ...interface{}) {
	if lion.LevelWarn < globalLevel {
		return
	}
	globalLogger.Warnln(args...)
}

// Errorf calls Errorf on the global Logger.
func Errorf(format string, args ...interface{}) {
	if lion.LevelError < globalLevel {
		return
	}
	globalLogger.Errorf(format, args...)
}

// Errorln calls Errorln on the global Logger.
func Errorln(args ...interface{}) {
	if lion.LevelError < globalLevel {
		return
	}
	globalLogger.Errorln(args...)
}

// Fatalf calls Fatalf on the global Logger.
func Fatalf(format string, args ...interface{}) {
	if lion.LevelFatal < globalLevel {
		return
	}
	globalLogger.Fatalf(format, args...)
}

// Fatalln calls Fatalln on the global Logger.
func Fatalln(args ...interface{}) {
	if lion.LevelFatal < globalLevel {
		return
	}
	globalLogger.Fatalln(args...)
}

// Panicf calls Panicf on the global Logger.
func Panicf(format string, args ...interface{}) {
	if lion.LevelPanic < globalLevel {
		return
	}
	globalLogger.Panicf(format, args...)
}

// Panicln calls Panicln on the global Logger.
func Panicln(args ...interface{}) {
	if lion.LevelPanic < globalLevel {
		return
	}
	globalLogger.Panicln(args...)
}

// Printf calls Printf on the global Logger.
func Printf(format string, args ...interface{}) {
	globalLogger.Printf(format, args...)
}

// Println calls Println on the global Logger.
func Println(args ...interface{}) {
	globalLogger.Println(args...)
}

// AtLevel calls AtLevel on the global Logger.
func AtLevel(level lion.Level) Logger {
	return globalLogger.AtLevel(level)
}

// WithField calls WithField on the global Logger.
func WithField(key string, value interface{}) Logger {
	return globalLogger.WithField(key, value)
}

// WithFields calls WithFields on the global Logger.
func WithFields(fields map[string]interface{}) Logger {
	return globalLogger.WithFields(fields)
}

// WithKeyValues calls WithKeyValues on the global Logger.
func WithKeyValues(keyValues ...interface{}) Logger {
	return globalLogger.WithKeyValues(keyValues...)
}

// WithContext calls WithContext on the global Logger.
func WithContext(context thrift.TStruct) Logger {
	return globalLogger.WithContext(context)
}

// Debug calls Debug on the global Logger.
func Debug(event thrift.TStruct) {
	if lion.LevelDebug < globalLevel {
		return
	}
	globalLogger.Debug(event)
}

// Info calls Info on the global Logger.
func Info(event thrift.TStruct) {
	if lion.LevelInfo < globalLevel {
		return
	}
	globalLogger.Info(event)
}

// Warn calls Warn on the global Logger.
func Warn(event thrift.TStruct) {
	if lion.LevelWarn < globalLevel {
		return
	}
	globalLogger.Warn(event)
}

// Error calls Error on the global Logger.
func Error(event thrift.TStruct) {
	if lion.LevelError < globalLevel {
		return
	}
	globalLogger.Error(event)
}

// Fatal calls Fatal on the global Logger.
func Fatal(event thrift.TStruct) {
	if lion.LevelFatal < globalLevel {
		return
	}
	globalLogger.Fatal(event)
}

// Panic calls Panic on the global Logger.
func Panic(event thrift.TStruct) {
	if lion.LevelPanic < globalLevel {
		return
	}
	globalLogger.Panic(event)
}

// Print calls Print on the global Logger.
func Print(event thrift.TStruct) {
	globalLogger.Print(event)
}

// LogDebug calls LogDebug on the global Logger.
func LogDebug() LevelLogger {
	return globalLogger.LogDebug()
}

// LogInfo calls LogInfo on the global Logger.
func LogInfo() LevelLogger {
	return globalLogger.LogInfo()
}

// LionLogger calls LionLogger on the global Logger.
func LionLogger() lion.Logger {
	return globalLogger.LionLogger()
}
