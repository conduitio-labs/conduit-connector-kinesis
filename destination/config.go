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

	// PartitionKeyTemplate consists of a go template, with the record being written
	// as the main context. If empty by default the partition key will adopt the record
	// key value.
	PartitionKeyTemplate string `json:"partitionKeyTemplate"`
}
