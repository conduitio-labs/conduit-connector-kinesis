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
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	testutils "github.com/conduitio-labs/conduit-connector-kinesis/test"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

func TestWrite_MultiStream_UsingColField(t *testing.T) {
	ctx := testContext(t)
	is := is.New(t)
	con := newDestination()
	stream1, cleanup1 := testutils.SetupTestStream(ctx, is)
	defer cleanup1()

	stream2, cleanup2 := testutils.SetupTestStream(ctx, is)
	defer cleanup2()

	stream3, cleanup3 := testutils.SetupTestStream(ctx, is)
	defer cleanup3()

	// streamName is fetched from `opencdc.collection` field
	configureDest(con, "")

	is.NoErr(con.Open(ctx))

	defer testutils.TeardownDestination(ctx, is, con)

	var recs []opencdc.Record
	recs1 := testRecordsStreamOnColField(t, stream1)
	recs = append(recs, recs1...)

	recs2 := testRecordsStreamOnColField(t, stream2)
	recs = append(recs, recs2...)

	recs3 := testRecordsStreamOnColField(t, stream3)
	recs = append(recs, recs3...)

	written, err := con.Write(ctx, recs)
	is.NoErr(err)
	is.Equal(written, len(recs))

	assertWrittenRecordsOnStream(ctx, is, stream1, recs1)
	assertWrittenRecordsOnStream(ctx, is, stream2, recs2)
	assertWrittenRecordsOnStream(ctx, is, stream3, recs3)

}

func TestWrite_MultiStream_UsingTemplate(t *testing.T) {
	ctx := testContext(t)
	is := is.New(t)

	con := newDestination()
	stream1, cleanup1 := testutils.SetupTestStream(ctx, is)
	defer cleanup1()

	stream2, cleanup2 := testutils.SetupTestStream(ctx, is)
	defer cleanup2()

	stream3, cleanup3 := testutils.SetupTestStream(ctx, is)
	defer cleanup3()

	template := `{{ index .Metadata "streamName" }}`
	configureDest(con, template)

	is.NoErr(con.Open(ctx))

	defer testutils.TeardownDestination(ctx, is, con)

	var recs []opencdc.Record
	recs1 := testRecordsStreamOnMetadata(t, stream1)
	recs = append(recs, recs1...)

	recs2 := testRecordsStreamOnMetadata(t, stream2)
	recs = append(recs, recs2...)

	recs3 := testRecordsStreamOnMetadata(t, stream3)
	recs = append(recs, recs3...)

	written, err := con.Write(ctx, recs)
	is.NoErr(err)
	is.Equal(written, len(recs))

	assertWrittenRecordsOnStream(ctx, is, stream1, recs1)
	assertWrittenRecordsOnStream(ctx, is, stream2, recs2)
	assertWrittenRecordsOnStream(ctx, is, stream3, recs3)

}

func TestTeardown_Open(t *testing.T) {
	ctx := testContext(t)
	is := is.New(t)
	con := newDestination()

	streamName, cleanup := testutils.SetupTestStream(ctx, is)
	defer cleanup()

	configureDest(con, streamName)

	is.NoErr(con.Open(ctx))

	testutils.TeardownDestination(ctx, is, con)
}

func TestWrite_PutRecords(t *testing.T) {
	ctx := testContext(t)
	is := is.New(t)
	con := newDestination()

	streamName, cleanup := testutils.SetupTestStream(ctx, is)
	defer cleanup()

	configureDest(con, streamName)

	is.NoErr(con.Open(ctx))

	defer testutils.TeardownDestination(ctx, is, con)

	cases := []struct {
		testName      string
		expectedError error
		records       []opencdc.Record
	}{
		{
			"happy path",
			nil,
			makeRecords(5, false),
		},
	}

	// setup table test
	for _, tt := range cases {
		recs := tt.records
		t.Run(tt.testName, func(_ *testing.T) {
			var err error
			if tt.expectedError != nil {
				// handle err
				is.Equal(err, tt.expectedError)
			}

			count, err := con.Write(ctx, recs)
			if err != nil {
				// handle err
				is.Equal(err, tt.expectedError)
			}

			is.NoErr(err)
			is.Equal(count, len(tt.records))

			recs := testutils.GetRecords(ctx, is, streamName)
			is.Equal(count, len(recs))
		})
	}
}

func TestWrite_PutRecord(t *testing.T) {
	ctx := testContext(t)
	is := is.New(t)
	con := newDestination()

	streamName, cleanup := testutils.SetupTestStream(ctx, is)
	defer cleanup()

	configureDest(con, streamName)

	is.NoErr(con.Open(ctx))

	defer testutils.TeardownDestination(ctx, is, con)

	cases := []struct {
		testName                 string
		expectedError            error
		expectedNumberOfRequests int
		records                  []opencdc.Record
	}{
		{
			"happy path - <500 records",
			nil,
			5,
			makeRecords(499, false),
		},
	}

	// setup table test
	for _, tt := range cases {
		t.Run(tt.testName, func(_ *testing.T) {
			count, err := con.Write(ctx, tt.records)
			if err != nil {
				// handle err
				is.Equal(err, tt.expectedError)
			}

			is.NoErr(err)
			is.Equal(count, len(tt.records))

			recs := testutils.GetRecords(ctx, is, con.config.StreamName)

			is.Equal(count, len(recs))
		})
	}
}

func makeRecords(count int, greaterThan5MB bool) []opencdc.Record {
	var records []opencdc.Record
	oneMB := (1024 * 1024) - 300000

	for i := 0; i < count; i++ {
		data := make([]byte, 16)
		_, _ = rand.Read(data)

		if greaterThan5MB {
			data = make([]byte, oneMB)
			_, _ = rand.Read(data)
		}
		key := strconv.Itoa(i)
		rec := sdk.Util.Source.NewRecordCreate(
			nil,
			nil,
			opencdc.RawData(key),
			opencdc.RawData(data),
		)

		records = append(records, rec)
	}
	return records
}

func assertWrittenRecordsOnStream(
	ctx context.Context, is *is.I,
	streamName string, records []opencdc.Record,
) {
	recs := testutils.GetRecords(ctx, is, streamName)
	is.Equal(len(records), len(recs))
}

func TestWrite_CreateStreamIfNotExists_SingleCollection(t *testing.T) {
	ctx := testContext(t)
	is := is.New(t)
	con := newDestination()
	testClient := testutils.NewTestClient(ctx, is)

	streamName := testutils.RandomStreamName("create_stream")
	defer testutils.DeleteStream(ctx, is, testClient, streamName)

	cfg := configureDest(con, streamName)
	cfg.AutoCreateStreams = true

	is.NoErr(con.Open(ctx))

	defer testutils.TeardownDestination(ctx, is, con)

	recs := testRecordsStreamOnColField(t, streamName)

	written, err := con.Write(ctx, recs)
	is.NoErr(err)
	is.Equal(written, len(recs))

	assertWrittenRecordsOnStream(ctx, is, streamName, recs)
}

func TestWrite_CreateStreamIfNotExists_MultiCollection(t *testing.T) {
	ctx := testContext(t)
	is := is.New(t)
	con := newDestination()
	testClient := testutils.NewTestClient(ctx, is)

	// create 1 stream beforehand and 2 stream names. The destination should
	// be able to write to the first stream without any
	// "streamAlreadyExists" error and create the 2 later streams.

	streamName1, cleanupStream1 := testutils.SetupTestStream(ctx, is)
	defer cleanupStream1()

	streamName2 := testutils.RandomStreamName("create_stream")
	defer testutils.DeleteStream(ctx, is, testClient, streamName2)

	streamName3 := testutils.RandomStreamName("create_stream")
	defer testutils.DeleteStream(ctx, is, testClient, streamName3)

	// streamName is fetched from `opencdc.collection` field
	cfg := configureDest(con, "")
	cfg.AutoCreateStreams = true

	is.NoErr(con.Open(ctx))

	defer testutils.TeardownDestination(ctx, is, con)

	var recs []opencdc.Record
	recs1 := testRecordsStreamOnColField(t, streamName1)
	recs = append(recs, recs1...)
	recs2 := testRecordsStreamOnColField(t, streamName2)
	recs = append(recs, recs2...)
	recs3 := testRecordsStreamOnColField(t, streamName3)
	recs = append(recs, recs3...)

	written, err := con.Write(ctx, recs)
	is.NoErr(err)
	is.Equal(written, len(recs))

	assertWrittenRecordsOnStream(ctx, is, streamName1, recs1)
	assertWrittenRecordsOnStream(ctx, is, streamName2, recs2)
	assertWrittenRecordsOnStream(ctx, is, streamName3, recs3)
}

func TestWrite_CreateStreamIfNotExists_AutoCreateFalseErrorsSingle(t *testing.T) {
	ctx := testContext(t)
	is := is.New(t)
	con := newDestination()

	streamName := testutils.RandomStreamName("create_stream")

	cfg := configureDest(con, streamName)
	cfg.AutoCreateStreams = false

	err := con.Open(ctx) // error is expected, since stream doesn't exist
	is.True(err != nil)

	// changing the public api just to check for a specific error message
	// feels a bit of an overkill, so instead we'll just check the error.

	errMsg := err.Error()
	want := fmt.Sprintf("stream %s does not exist", streamName)
	if !strings.Contains(errMsg, want) {
		t.Fatalf("expected error to contain %s, got %s", want, errMsg)
	}
}

func TestWrite_CreateStreamIfNotExists_AutoCreateFalseErrorsMulti(t *testing.T) {
	ctx := testContext(t)
	is := is.New(t)
	con := newDestination()

	cfg := configureDest(con, "")
	cfg.AutoCreateStreams = false

	is.NoErr(con.Open(ctx))
	defer testutils.TeardownDestination(ctx, is, con)

	recs := testRecordsStreamOnColField(t, "non-existent-stream")

	written, err := con.Write(ctx, recs)
	is.True(err != nil)
	is.Equal(written, 0)

	// Here, instead of checking the error message, we'll just check the
	// error type. The write method directly interacts with the AWS SDK,
	// so we can just check the error type.

	var notFoundErr *types.ResourceNotFoundException
	if !errors.As(err, &notFoundErr) {
		t.Fatalf("expected error to be of type %T, got %T", notFoundErr, err)
	}
}

func testContext(t *testing.T) context.Context {
	logger := zerolog.New(zerolog.NewTestWriter(t))
	return logger.WithContext(context.Background())
}
