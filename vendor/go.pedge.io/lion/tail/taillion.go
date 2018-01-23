/*
Package taillion implements tailing of lion log files.

Uses https://github.com/hpcloud/tail.
*/
package taillion

import (
	"bytes"

	"github.com/hpcloud/tail"
	"go.pedge.io/lion"
)

// EntryHandler handles a lion.Entry that was read.
type EntryHandler func(*lion.Entry) error

// ErrorHandler handles an error from tailing/
// If it returns true, Tail should return.
type ErrorHandler func(error) bool

// SeekInfo represents arguments to `os.Seek`
type SeekInfo struct {
	Offset int64
	Whence int // os.SEEK_*
}

// TailOptions are options to Tail.
type TailOptions struct {
	Location  *SeekInfo     // Seek to this location before tailing
	ReOpen    bool          // Reopen recreated files (tail -F)
	MustExist bool          // Fail early if the file does not exist
	Poll      bool          // Poll for file changes instead of using inotify
	Pipe      bool          // Is a named pipe (mkfifo)
	Follow    bool          // Continue looking for new lines (tail -f)
	Done      chan struct{} // Stop when this channel closes
}

// Tail tails the given file for entries.
func Tail(
	filePath string,
	unmarshaller lion.Unmarshaller,
	entryHandler EntryHandler,
	errorHandler ErrorHandler, // if nil, Tail will just return the error
	options TailOptions,
) error {
	t, err := tail.TailFile(filePath, tailOptionsToConfig(options))
	if err != nil {
		return err
	}
	if options.Done != nil {
		for {
			select {
			case <-options.Done:
				err = t.Stop()
				t.Cleanup()
				return err
			case line := <-t.Lines:
				if err := handleLine(
					unmarshaller,
					entryHandler,
					errorHandler,
					line.Text,
				); err != nil {
					return err
				}
			}
		}
	} else {
		for line := range t.Lines {
			if err := handleLine(
				unmarshaller,
				entryHandler,
				errorHandler,
				line.Text,
			); err != nil {
				return err
			}
		}
	}
	t.Cleanup()
	return nil
}

func tailOptionsToConfig(tailOptions TailOptions) tail.Config {
	config := tail.Config{
		ReOpen:    tailOptions.ReOpen,
		MustExist: tailOptions.MustExist,
		Poll:      tailOptions.Poll,
		Pipe:      tailOptions.Pipe,
		Follow:    tailOptions.Follow,
	}
	if tailOptions.Location != nil {
		config.Location = &tail.SeekInfo{
			Offset: tailOptions.Location.Offset,
			Whence: tailOptions.Location.Whence,
		}
	}
	return config
}

func handleLine(
	unmarshaller lion.Unmarshaller,
	entryHandler EntryHandler,
	errorHandler ErrorHandler,
	line string,
) error {
	entry, err := lineToEntry(unmarshaller, line)
	if err != nil {
		if errorHandler == nil || errorHandler(err) {
			return err
		}
	} else {
		if err := entryHandler(entry); err != nil {
			if errorHandler == nil || errorHandler(err) {
				return err
			}
		}
	}
	return nil
}

func lineToEntry(unmarshaller lion.Unmarshaller, line string) (*lion.Entry, error) {
	encodedEntry := &lion.EncodedEntry{}
	if err := unmarshaller.Unmarshal(bytes.NewReader([]byte(line)), encodedEntry); err != nil {
		return nil, err
	}
	return encodedEntry.Decode()
}
