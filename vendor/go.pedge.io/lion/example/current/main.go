/*
Package main implements an example integration with Current.

https://current.sh

To use with syslog, run ./syslog.bash <org>.

To use with the current cli tool, run using ./stdout.bash <org>.

To output the debug messages, set the LOG_LEVEL environment variable to DEBUG.
*/
package main

import (
	"fmt"
	"os"

	"go.pedge.io/lion/env"
	"go.pedge.io/lion/proto"
)

func main() {
	if err := do(); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	os.Exit(0)
}

func do() error {
	if err := envlion.Setup(); err != nil {
		return err
	}
	for i := 1; i < 5; i++ {
		protolion.Info(
			&Foo{
				Bar: &Bar{
					One: "one",
				},
				Two:   fmt.Sprintf("two%d", i),
				Three: uint64(i),
			},
		)
		protolion.Debugf("hello%d", i*i*i)
	}
	return nil
}
