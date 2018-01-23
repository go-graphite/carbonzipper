package thriftliontesting

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"

	"go.pedge.io/lion"
	"go.pedge.io/lion/kit"
	"go.pedge.io/lion/tail"
	"go.pedge.io/lion/thrift"

	"github.com/stretchr/testify/require"
)

func init() {
	thriftlion.MustRegister(NewFoo)
	thriftlion.MustRegister(NewBar)
	thriftlion.MustRegister(NewBaz)
	thriftlion.MustRegister(NewBat)
	thriftlion.MustRegister(NewBan)
	thriftlion.MustRegister(NewEmpty)
	thriftlion.MustRegister(NewNoStdJson)
}

func TestRoundtripAndTextMarshaller(t *testing.T) {
	testBothRoundTripAndTextMarshaller(
		t,
		func(logger thriftlion.Logger) {
			logger.Debug(
				&Foo{
					StringField: thrift.StringPtr("one"),
					Int32Field:  thrift.Int32Ptr(2),
				},
			)
			logger.LogDebug().WithField("1", "2").Println("hello")
			logger.Info(
				&Foo{
					StringField: thrift.StringPtr("one"),
					Int32Field:  thrift.Int32Ptr(2),
				},
			)
			logger.LogInfo().WithField("3", "4").Println("hello2")
			logger.Info(
				&Baz{
					Bat: &Bat{
						Ban: &Ban{
							StringField: thrift.StringPtr("one"),
							Int32Field:  thrift.Int32Ptr(2),
						},
					},
				},
			)
			logger.Info(&Empty{})
			logger.Info(&NoStdJson{
				One: map[int64]string{
					1: "one",
					2: "two",
				},
			})
			writer := logger.InfoWriter()
			for _, s := range []string{
				"hello",
				"world",
				"writing",
				"strings",
				"is",
				"fun",
			} {
				_, _ = writer.Write([]byte(s))
			}
			writer = logger.Writer()
			_, _ = writer.Write([]byte("none"))
			logger.Infoln("a normal line")
			logger.WithField("someKey", "someValue").Warnln("a warning line")
		},
		`INFO  lion.testing.Foo {"string_field":"one","int32_field":2}
INFO  hello2 {"3":"4"}
INFO  lion.testing.Baz {"bat":{"ban":{"string_field":"one","int32_field":2}}}
INFO  lion.testing.Empty {}
INFO  lion.testing.NoStdJson {"one":{"1":"one","2":"two"}}
INFO  hello
INFO  world
INFO  writing
INFO  strings
INFO  is
INFO  fun
NONE  none
INFO  a normal line
WARN  a warning line {"someKey":"someValue"}
`,
	)
}

func TestLevelNone(t *testing.T) {
	testBothRoundTripAndTextMarshaller(
		t,
		func(logger thriftlion.Logger) {
			logger = logger.AtLevel(lion.LevelPanic)
			logger.Errorln("hello")
			logger.Println("hello")
			logger = logger.AtLevel(lion.LevelNone)
			logger.Println("hello2")
		},
		"NONE  hello\nNONE  hello2\n",
	)
}

func testBothRoundTripAndTextMarshaller(t *testing.T, f func(thriftlion.Logger), expected string) {
	testRoundTripAndTextMarshaller(t, f, expected)
	testRoundTripAndTextMarshallerTail(t, f, expected)
}

func testRoundTripAndTextMarshaller(t *testing.T, f func(thriftlion.Logger), expected string) {
	for _, marshalPair := range []struct {
		marshaller   lion.Marshaller
		unmarshaller lion.Unmarshaller
	}{} {
		buffer := bytes.NewBuffer(nil)
		fakeTimer := newFakeTimer(0)
		logger := thriftlion.NewLogger(
			lion.NewLogger(
				lion.NewWritePusher(
					buffer,
					marshalPair.marshaller,
				),
				lion.LoggerWithIDAllocator(newFakeIDAllocator()),
				lion.LoggerWithTimer(fakeTimer),
			),
		)
		f(logger)
		puller := lion.NewReadPuller(
			buffer,
			marshalPair.unmarshaller,
		)
		writeBuffer := bytes.NewBuffer(nil)
		writePusher := lion.NewTextWritePusher(
			writeBuffer,
			lion.TextMarshallerDisableTime(),
		)
		for encodedEntry, pullErr := puller.Pull(); pullErr != io.EOF; encodedEntry, pullErr = puller.Pull() {
			require.NoError(t, pullErr)
			entry, err := encodedEntry.Decode()
			require.NoError(t, err)
			require.NoError(t, writePusher.Push(entry))
		}
		require.Equal(t, expected, writeBuffer.String())
	}
}

func testRoundTripAndTextMarshallerTail(t *testing.T, f func(thriftlion.Logger), expected string) {
	t.Skip()
	file, err := ioutil.TempFile("", "lion")
	require.NoError(t, err)
	filePath := file.Name()
	// will not actually get called if a require statement is hit
	defer func() {
		_ = file.Close()
		_ = os.Remove(filePath)

	}()
	fakeTimer := newFakeTimer(0)
	logger := thriftlion.NewLogger(
		lion.NewLogger(
			lion.NewWritePusher(
				file,
				//thriftlion.Base64DelimitedNewlineMarshaller,
				nil,
			),
			lion.LoggerWithIDAllocator(newFakeIDAllocator()),
			lion.LoggerWithTimer(fakeTimer),
		),
	)
	f(logger)
	writeBuffer := bytes.NewBuffer(nil)
	writePusher := lion.NewTextWritePusher(
		writeBuffer,
		lion.TextMarshallerDisableTime(),
	)
	require.NoError(
		t,
		taillion.Tail(
			filePath,
			//thriftlion.Base64DelimitedUnmarshaller,
			nil,
			writePusher.Push,
			nil,
			taillion.TailOptions{},
		),
	)
	require.Equal(t, expected, writeBuffer.String())
}

func TestPrintSomeStuff(t *testing.T) {
	testPrintSomeStuff(t, thriftlion.NewLogger(lion.DefaultLogger))
}

func TestPrintSomeStuffJSON(t *testing.T) {
	testPrintSomeStuff(t, thriftlion.NewLogger(lion.NewLogger(lion.NewJSONWritePusher(os.Stderr))))
}

func testPrintSomeStuff(t *testing.T, logger thriftlion.Logger) {
	logger.Info(
		&Foo{
			StringField: thrift.StringPtr("one"),
			Int32Field:  thrift.Int32Ptr(2),
		},
	)
	logger.Info(
		&Baz{
			Bat: &Bat{
				Ban: &Ban{
					StringField: thrift.StringPtr("one"),
					Int32Field:  thrift.Int32Ptr(2),
				},
			},
		},
	)
	writer := logger.InfoWriter()
	for _, s := range []string{
		"hello",
		"world",
		"writing",
		"strings",
		"is",
		"fun",
	} {
		_, _ = writer.Write([]byte(s))
	}
	writer = logger.Writer()
	_, _ = writer.Write([]byte("none"))
	logger.Infoln("a normal line")
	logger.WithField("someKey", "someValue").WithField("someOtherKey", 1).Warnln("a warning line")
	logger.WithField("someKey", "someValue").WithField("someOtherKey", 1).Info(
		&Baz{
			Bat: &Bat{
				Ban: &Ban{
					StringField: thrift.StringPtr("one"),
					Int32Field:  thrift.Int32Ptr(2),
				},
			},
		},
	)
	logger.WithKeyValues("someKey", "someValue", "someOtherKey", 1).Infoln()
	_ = kitlion.NewLogger(logger.LionLogger()).Log("someKey", "someValue", "someOtherKey", 1)
}

type fakeIDAllocator struct {
	value int32
}

func newFakeIDAllocator() *fakeIDAllocator {
	return &fakeIDAllocator{-1}
}

func (f *fakeIDAllocator) Allocate() string {
	return fmt.Sprintf("%d", atomic.AddInt32(&f.value, 1))
}

type fakeTimer struct {
	unixTimeUsec int64
}

func newFakeTimer(initialUnixTimeUsec int64) *fakeTimer {
	return &fakeTimer{initialUnixTimeUsec}
}

func (f *fakeTimer) Now() time.Time {
	return time.Unix(f.unixTimeUsec/int64(time.Second), f.unixTimeUsec%int64(time.Second)).UTC()
}

func (f *fakeTimer) Add(secondDelta int64, nanosecondDelta int64) {
	atomic.AddInt64(&f.unixTimeUsec, (secondDelta*int64(time.Second))+nanosecondDelta)
}
