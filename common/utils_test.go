package common

import (
	"testing"
	"time"

	"github.com/matryer/is"
)

func TestExponentialBackoff(t *testing.T) {
	is := is.New(t)

	start := time.Now()

	wait := ExponentialBackoff(time.Millisecond)
	for i := 0; i < 3; i++ {
		wait()
	}

	elapsed := time.Since(start)

	// elapsed should be 2 ** 3 * time.Millisecond (~8ms)
	is.True(elapsed > 7*time.Millisecond)
	is.True(elapsed < 9*time.Millisecond)
}
