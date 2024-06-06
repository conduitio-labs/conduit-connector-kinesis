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

	// UseMultiStreamMode allows the destination to write to multiple streams.
	// If set to true, by default the destination will use the streamARN from
	// the opencdc.collection metadata field. If no collection is found, the
	// destination will use the StreamARN parameter as the last resort. Use
	// StreamARNTemplate to customise how the streamARN is parsed from the
	// record.
	UseMultiStreamMode bool `json:"useMultiStreamMode" default:"false"`

	// StreamARNTemplate allows the destination to use a custom go template to
	// parse the streamARN. The template will be given an sdk.Record as the main
	// data context.
	StreamARNTemplate string `json:"streamARNTemplate"`

	// PartitionKeyTemplate accepts a go template as an argument, with the
	// record being written as the main data context. If an empty template is
	// passed, the partition key will adopt the record key string value. If the
	// record key string exceeds 256 it will be trimmed down from start to fit
	// the partition key size
	PartitionKeyTemplate string `json:"partitionKeyTemplate"`
}
