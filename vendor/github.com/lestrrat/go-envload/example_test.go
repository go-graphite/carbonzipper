package envload_test

import (
	"fmt"
	"os"

	envload "github.com/lestrrat/go-envload"
)

func Example() {
	os.Setenv("FOO", "foo")
	os.Setenv("BAR", "bar")
	fmt.Printf("FOO = %s\n", os.Getenv("FOO"))
	fmt.Printf("BAR = %s\n", os.Getenv("BAR"))

	loader := envload.New()

	os.Setenv("FOO", "Hello")
	os.Setenv("BAR", "World!")
	fmt.Printf("FOO = %s\n", os.Getenv("FOO"))
	fmt.Printf("BAR = %s\n", os.Getenv("BAR"))

	if err := loader.Restore(); err != nil {
		fmt.Printf("error while restoring environemnt: %s\n", err)
		return
	}

	fmt.Printf("FOO = %s\n", os.Getenv("FOO"))
	fmt.Printf("BAR = %s\n", os.Getenv("BAR"))
	// Output:
	// FOO = foo
	// BAR = bar
	// FOO = Hello
	// BAR = World!
	// FOO = foo
	// BAR = bar
}
