package common

import (
	"time"
)

// ExponentialBackoffDelay returns a function that will sleep for a given
// delay, and then exponentially double the delay for the next call. This
// is not concurrently safe.
// Example usage:
//
//	wait := ExponentialBackoffDelay(time.Second)
//	wait() // sleep for 1 second
//	wait() // sleep for 2 seconds
//	wait() // sleep for 4 seconds
//	...
func ExponentialBackoff(delay time.Duration) func() {
	twoExp := 1

	return func() {
		time.Sleep(delay * time.Duration(twoExp))
		twoExp *= 2
	}
}
