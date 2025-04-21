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

package source

import (
	"github.com/conduitio-labs/conduit-connector-kinesis/common"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Config struct {
	sdk.DefaultSourceMiddleware

	common.Config

	// The name of the Kinesis Data Stream.
	StreamName string `json:"streamName" validate:"required"`

	// If true, sets the iterator type to LATEST (iterates from the point
	// that the connection begins = CDC). Otherwise it sets the iterator type
	// to TRIM_HORIZON (iterates from the oldest record in the shard = snapshot).
	// Iterators eventually shift to latest after snapshot has been written.
	StartFromLatest bool `json:"startFromLatest"`
}
