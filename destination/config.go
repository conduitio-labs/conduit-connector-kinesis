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

package destination

import "github.com/conduitio-labs/conduit-connector-kinesis/common"

//go:generate paramgen -output=config_paramgen.go Config
type Config struct {
	// Config includes parameters that are the same in the source and destination.
	common.Config

	// UseSingleShard is a boolean that determines whether to add records in batches using KinesisClient.PutRecords
	// (ordering not guaranteed, records written to multiple shards)
	// or to add records to a single shard using KinesisClient.PutRecord
	// (preserves ordering, records written to a single shard)
	// Defaults to false to take advantage of batching performance
	UseSingleShard bool `json:"useSingleShard" validate:"required" default:"true"`
}
