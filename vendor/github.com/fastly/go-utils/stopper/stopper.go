package stopper

// Stopper is an embeddable interface for objects ("stoppees") which have
// goroutines that need to be stopped at the object's owner's request.
type Stopper interface {
	// Stop asks the stoppee to stop its goroutines.
	Stop()
	// OnDone registers a callback to be fired once the stop is complete.
	OnDone(func())
	// Finish is called by the stoppee when it has shut down.
	Finish()
	// Done returns true only after Done has been called.
	Done() bool
}
