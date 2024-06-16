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
