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

	// StreamName is the name of the Kinesis Data Stream. It can contain a
	// [Go template](https://pkg.go.dev/text/template) that will be executed
	// for each record to determine the stream name. By default, the stream
	// name is the value of the `opencdc.collection` metadata field.
	StreamName string `json:"streamName" default:"{{ index .Metadata \"opencdc.collection\" }}"`

	// PartitionKeyTemplate accepts a go template as an argument, with the
	// record being written as the main data context. If an empty template is
	// passed, the partition key will adopt the record key string value. If the
	// record key string exceeds 256 it will be trimmed down from start to fit
	// the partition key size
	PartitionKeyTemplate string `json:"partitionKeyTemplate"`
}
