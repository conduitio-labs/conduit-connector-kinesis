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
		parser := &fromColFieldParser{defaultStreamARN: "test-stream-arn"}

		batches, err := parseBatches([]sdk.Record{}, parser)
		is.NoErr(err)

		is.Equal(0, len(batches))
	})

	t.Run("only one stream given", func(t *testing.T) {
		is := is.New(t)
		parser := &fromColFieldParser{defaultStreamARN: "test-stream-arn"}

		var records []sdk.Record
		recs1 := testRecords(t)
		records = append(records, recs1...)

		recs2 := testRecordsStreamOnColField(t, "test-stream-arn-2")
		records = append(records, recs2...)

		recs3 := testRecords(t)
		records = append(records, recs3...)

		batches, err := parseBatches(records, parser)
		is.NoErr(err)

		is.Equal(3, len(batches))

		is.Equal(recs1, batches[0].records)
		is.Equal("test-stream-arn", batches[0].streamARN)

		is.Equal(recs2, batches[1].records)
		is.Equal("test-stream-arn-2", batches[1].streamARN)

		is.Equal(recs3, batches[2].records)
		is.Equal("test-stream-arn", batches[2].streamARN)
	})

	t.Run("different streams given", func(t *testing.T) {
		is := is.New(t)

		parser, err := newFromTemplateParser(`{{ index .Metadata "streamARN" }}`)
		is.NoErr(err)

		var records []sdk.Record
		recs1 := testRecordsStreamOnMetadata(t, "test-stream-arn-1")
		records = append(records, recs1...)

		recs2 := testRecordsStreamOnMetadata(t, "test-stream-arn-2")
		records = append(records, recs2...)

		recs3 := testRecordsStreamOnMetadata(t, "test-stream-arn-3")
		records = append(records, recs3...)

		batches, err := parseBatches(records, parser)
		is.NoErr(err)

		is.Equal(3, len(batches))

		is.Equal(recs1, batches[0].records)
		is.Equal("test-stream-arn-1", batches[0].streamARN)

		is.Equal(recs2, batches[1].records)
		is.Equal("test-stream-arn-2", batches[1].streamARN)

		is.Equal(recs3, batches[2].records)
		is.Equal("test-stream-arn-3", batches[2].streamARN)
	})
}

func testRecords(t *testing.T) []sdk.Record {
	var testDriver sdk.ConfigurableAcceptanceTestDriver

	var records []sdk.Record

	for range rand.Intn(3) + 1 {
		rec := testDriver.GenerateRecord(t, sdk.OperationCreate)
		records = append(records, rec)
	}

	return records
}

func testRecordsStreamOnColField(t *testing.T, streamARN string) []sdk.Record {
	var testDriver sdk.ConfigurableAcceptanceTestDriver

	var records []sdk.Record

	for range rand.Intn(3) + 1 {
		rec := testDriver.GenerateRecord(t, sdk.OperationCreate)
		records = append(records, rec)
		rec.Metadata.SetCollection(streamARN)
	}

	return records
}

func testRecordsStreamOnMetadata(t *testing.T, streamARN string) []sdk.Record {
	var testDriver sdk.ConfigurableAcceptanceTestDriver

	var records []sdk.Record

	for range rand.Intn(3) + 1 {
		rec := testDriver.GenerateRecord(t, sdk.OperationCreate)
		records = append(records, rec)
		rec.Metadata["streamARN"] = streamARN
	}

	return records
}
