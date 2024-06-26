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
	"strconv"
	"testing"

	testutils "github.com/conduitio-labs/conduit-connector-kinesis/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

func TestWrite_MultiStream(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t))
	ctx := logger.WithContext(context.Background())

	t.Run("using col field", func(t *testing.T) {
		is := is.New(t)
		con := newDestination()
		stream1, cleanup1 := testutils.SetupTestStream(ctx, is)
		defer cleanup1()

		stream2, cleanup2 := testutils.SetupTestStream(ctx, is)
		defer cleanup2()

		stream3, cleanup3 := testutils.SetupTestStream(ctx, is)
		defer cleanup3()

		// streamName is fetched from `opencdc.collection` field
		err := con.Configure(ctx, testutils.GetTestConfig(""))
		is.NoErr(err)

		err = con.Open(ctx)
		is.NoErr(err)

		defer testutils.TeardownDestination(ctx, is, con)

		var recs []sdk.Record
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
	})

	t.Run("using template", func(t *testing.T) {
		is := is.New(t)

		con := newDestination()
		stream1, cleanup1 := testutils.SetupTestStream(ctx, is)
		defer cleanup1()

		stream2, cleanup2 := testutils.SetupTestStream(ctx, is)
		defer cleanup2()

		stream3, cleanup3 := testutils.SetupTestStream(ctx, is)
		defer cleanup3()

		template := `{{ index .Metadata "streamName" }}`
		err := con.Configure(ctx, testutils.GetTestConfig(template))
		is.NoErr(err)

		err = con.Open(ctx)
		is.NoErr(err)

		defer testutils.TeardownDestination(ctx, is, con)

		var recs []sdk.Record
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
	})
}

func TestTeardown_Open(t *testing.T) {
	is := is.New(t)
	logger := zerolog.New(zerolog.NewTestWriter(t))
	ctx := logger.WithContext(context.Background())
	con := newDestination()

	streamName, cleanup := testutils.SetupTestStream(ctx, is)
	defer cleanup()

	err := con.Configure(ctx, testutils.GetTestConfig(streamName))
	is.NoErr(err)

	err = con.Open(ctx)
	is.NoErr(err)

	testutils.TeardownDestination(ctx, is, con)
}

func TestWrite_PutRecords(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t))
	ctx := logger.WithContext(context.Background())
	is := is.New(t)
	con := newDestination()

	streamName, cleanup := testutils.SetupTestStream(ctx, is)
	defer cleanup()

	err := con.Configure(ctx, testutils.GetTestConfig(streamName))
	is.NoErr(err)

	err = con.Open(ctx)
	is.NoErr(err)

	defer testutils.TeardownDestination(ctx, is, con)

	cases := []struct {
		testName      string
		expectedError error
		records       []sdk.Record
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
	logger := zerolog.New(zerolog.NewTestWriter(t))
	ctx := logger.WithContext(context.Background())
	is := is.New(t)
	con := newDestination()

	streamName, cleanup := testutils.SetupTestStream(ctx, is)
	defer cleanup()

	err := con.Configure(ctx, testutils.GetTestConfig(streamName))
	is.NoErr(err)

	err = con.Open(ctx)
	is.NoErr(err)

	defer testutils.TeardownDestination(ctx, is, con)

	cases := []struct {
		testName                 string
		expectedError            error
		expectedNumberOfRequests int
		records                  []sdk.Record
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

func makeRecords(count int, greaterThan5MB bool) []sdk.Record {
	var records []sdk.Record
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
			sdk.RawData(key),
			sdk.RawData(data),
		)

		records = append(records, rec)
	}
	return records
}

func assertWrittenRecordsOnStream(
	ctx context.Context, is *is.I,
	streamName string, records []sdk.Record,
) {
	recs := testutils.GetRecords(ctx, is, streamName)
	is.Equal(len(records), len(recs))
}

func TestWrite_CreateStreamIfNotExists(t *testing.T) {
	getTestConfig := func(streamName string) map[string]string {
		cfg := testutils.GetTestConfig(streamName)
		cfg["createIfNotExists"] = "true"
		return cfg
	}

	t.Run("create stream in single collection mode", func(t *testing.T) {
		logger := zerolog.New(zerolog.NewTestWriter(t))
		ctx := logger.WithContext(context.Background())
		is := is.New(t)
		con := newDestination()
		testClient := testutils.NewTestClient(ctx, is)

		streamName := testutils.RandomStreamName("create_stream")
		defer testutils.DeleteStream(ctx, is, testClient, streamName)

		err := con.Configure(ctx, getTestConfig(streamName))
		is.NoErr(err)

		err = con.Open(ctx)
		is.NoErr(err)

		defer testutils.TeardownDestination(ctx, is, con)

		recs := testRecordsStreamOnColField(t, streamName)

		written, err := con.Write(ctx, recs)
		is.NoErr(err)
		is.Equal(written, len(recs))

		assertWrittenRecordsOnStream(ctx, is, streamName, recs)
	})

	t.Run("create stream in multi collection mode", func(t *testing.T) {
		logger := zerolog.New(zerolog.NewTestWriter(t))
		ctx := logger.WithContext(context.Background())
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
		err := con.Configure(ctx, getTestConfig(""))
		is.NoErr(err)

		err = con.Open(ctx)
		is.NoErr(err)

		defer testutils.TeardownDestination(ctx, is, con)

		var recs []sdk.Record
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
	})
}
