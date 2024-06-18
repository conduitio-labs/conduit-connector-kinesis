// Copyright © 2024 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
