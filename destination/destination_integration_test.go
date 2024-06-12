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
	"net/http"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/conduitio-labs/conduit-connector-kinesis/common"
	testutils "github.com/conduitio-labs/conduit-connector-kinesis/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

func TestWrite_MultiStream(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t))
	ctx := logger.WithContext(context.Background())
	is := is.New(t)
	con := newDestination()

	// we make sure that the client is independent from the destination
	testClient, err := common.NewClient(ctx, &http.Client{}, common.Config{
		AWSRegion:          "us-east-1",
		AWSAccessKeyID:     "accesskeymock",
		AWSSecretAccessKey: "accesssecretmock",
		AWSURL:             "http://localhost:4566",
	})
	is.NoErr(err)

	stream1, cleanup1 := testutils.SetupTestStream(ctx, is, testClient)
	defer cleanup1()

	stream2, cleanup2 := testutils.SetupTestStream(ctx, is, testClient)
	defer cleanup2()

	stream3, cleanup3 := testutils.SetupTestStream(ctx, is, testClient)
	defer cleanup3()

	err = con.Configure(ctx, testutils.GetTestConfig(stream1))
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

	t.Log("asserting stream 1", stream1)
	assertWrittenRecordsOnStream(is, con.client, stream1, recs1)
	t.Log("asserting stream 2", stream2)
	assertWrittenRecordsOnStream(is, con.client, stream2, recs2)
	t.Log("asserting stream 3", stream3)
	assertWrittenRecordsOnStream(is, con.client, stream3, recs3)
}

func TestTeardown_Open(t *testing.T) {
	is := is.New(t)
	logger := zerolog.New(zerolog.NewTestWriter(t))
	ctx := logger.WithContext(context.Background())
	con := &Destination{
		client:     &kinesis.Client{},
		httpClient: &http.Client{},
	}

	streamName, cleanup := testutils.SetupTestStream(ctx, is, con.client)
	defer cleanup()

	err := con.Configure(ctx, testutils.GetTestConfig(streamName))
	is.NoErr(err)

	err = con.Open(ctx)
	is.NoErr(err)

	defer testutils.TeardownDestination(ctx, is, con)
}

func TestWrite_PutRecords(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t))
	ctx := logger.WithContext(context.Background())
	is := is.New(t)
	con := &Destination{
		client:     &kinesis.Client{},
		httpClient: &http.Client{},
	}

	streamName, cleanup := testutils.SetupTestStream(ctx, is, con.client)
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

			recs := testutils.GetRecords(ctx, is, streamName, con.client)
			is.Equal(count, len(recs))
		})
	}
}

func TestWrite_PutRecord(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t))
	ctx := logger.WithContext(context.Background())
	is := is.New(t)
	con := newDestination()

	streamName, cleanup := testutils.SetupTestStream(ctx, is, con.client)
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

			recs := testutils.GetRecords(ctx, is, con.config.StreamName, con.client)

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
	is *is.I,
	client *kinesis.Client,
	streamName string,
	records []sdk.Record,
) {
	ctx := context.Background()
	recs := testutils.GetRecords(ctx, is, streamName, client)
	is.Equal(len(records), len(recs))
}
