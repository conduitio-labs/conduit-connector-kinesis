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

import (
	"math/rand"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func TestParseBatches(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		is := is.New(t)
		parser := &fromColFieldParser{}

		batches, err := parseBatches([]sdk.Record{}, parser)
		is.NoErr(err)

		is.Equal(0, len(batches))
	})

	t.Run("only one stream given using fromColFieldParser", func(t *testing.T) {
		is := is.New(t)
		parser := &fromColFieldParser{}

		var records []sdk.Record
		recs1 := testRecordsStreamOnColField(t, "test-stream-name-1")
		records = append(records, recs1...)

		recs2 := testRecordsStreamOnColField(t, "test-stream-name-2")
		records = append(records, recs2...)

		recs3 := testRecordsStreamOnColField(t, "test-stream-name-3")
		records = append(records, recs3...)

		batches, err := parseBatches(records, parser)
		is.NoErr(err)

		is.Equal(3, len(batches))

		is.Equal(recs1, batches[0].records)
		is.Equal("test-stream-name-1", batches[0].streamName)

		is.Equal(recs2, batches[1].records)
		is.Equal("test-stream-name-2", batches[1].streamName)

		is.Equal(recs3, batches[2].records)
		is.Equal("test-stream-name-3", batches[2].streamName)
	})

	t.Run("different streams given using fromTemplateParser", func(t *testing.T) {
		is := is.New(t)

		parser, err := newFromTemplateParser(`{{ index .Metadata "streamName" }}`)
		is.NoErr(err)

		var records []sdk.Record
		recs1 := testRecordsStreamOnMetadata(t, "test-stream-name-1")
		records = append(records, recs1...)

		recs2 := testRecordsStreamOnMetadata(t, "test-stream-name-2")
		records = append(records, recs2...)

		recs3 := testRecordsStreamOnMetadata(t, "test-stream-name-3")
		records = append(records, recs3...)

		batches, err := parseBatches(records, parser)
		is.NoErr(err)

		is.Equal(3, len(batches))

		is.Equal(recs1, batches[0].records)
		is.Equal("test-stream-name-1", batches[0].streamName)

		is.Equal(recs2, batches[1].records)
		is.Equal("test-stream-name-2", batches[1].streamName)

		is.Equal(recs3, batches[2].records)
		is.Equal("test-stream-name-3", batches[2].streamName)
	})
}

func testRecordsStreamOnColField(t *testing.T, streamName string) []sdk.Record {
	var testDriver sdk.ConfigurableAcceptanceTestDriver

	var records []sdk.Record
	for range rand.Intn(3) + 1 {
		rec := testDriver.GenerateRecord(t, sdk.OperationCreate)
		rec.Metadata.SetCollection(streamName)

		records = append(records, rec)
	}

	return records
}

func testRecordsStreamOnMetadata(t *testing.T, streamName string) []sdk.Record {
	var testDriver sdk.ConfigurableAcceptanceTestDriver

	var records []sdk.Record

	for range rand.Intn(3) + 1 {
		rec := testDriver.GenerateRecord(t, sdk.OperationCreate)
		records = append(records, rec)
		rec.Metadata["streamName"] = streamName
	}

	return records
}
