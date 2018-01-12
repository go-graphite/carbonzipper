package envload_test

import (
	"os"
	"testing"
	"time"

	envload "github.com/lestrrat/go-envload"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestIter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	src := []string{`FOO=foo`, `BAR=bar`, `BAZ=baz`}
	l := envload.New(src...)
	i := l.Iterator(ctx)
	if !assert.NotNil(t, i, "Iterator is ok") {
		return
	}

	os.Setenv(`QUUX`, `quux`) // This should have no effect
	var list []string
	for i.Next() {
		k, v := i.KV()
		t.Logf("%s=%v", k, v)
		list = append(list, k+"="+v)
	}

	if !assert.Equal(t, src, list) {
		return
	}
}

func TestEnviron(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	src := []string{`FOO=foo`, `BAR=bar`, `BAZ=baz`}
	l := envload.New(src...)
	i := l.Iterator(ctx)
	if !assert.NotNil(t, i, "Iterator is ok") {
		return
	}

	os.Setenv(`QUUX`, `quux`) // This should have no effect
	list := l.Environ(ctx)
	if !assert.Equal(t, src, list) {
		return
	}
}
