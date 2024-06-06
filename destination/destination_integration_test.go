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
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/conduitio-labs/conduit-connector-kinesis/common"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	"github.com/oklog/ulid/v2"
	"github.com/rs/zerolog"
)

var cfg = map[string]string{
	"streamARN":           "arn:aws:kinesis:us-east-1:000000000000:stream/stream1",
	"aws.region":          "us-east-1",
	"aws.accessKeyId":     "accesskeymock",
	"aws.secretAccessKey": "accesssecretmock",
	"aws.url":             "http://localhost:4566",
}

func TestWrite_MultiStream(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t))
	ctx := logger.WithContext(context.Background())
	is := is.New(t)
	con := newDestination()

	// we make sure that the client is independent from the destination
	testClient, err := newClient(ctx, &http.Client{}, common.Config{
		AWSRegion:          "us-east-1",
		AWSAccessKeyID:     "accesskeymock",
		AWSSecretAccessKey: "accesssecretmock",
		AWSURL:             "http://localhost:4566",
	})
	is.NoErr(err)

	stream1 := setupDestinationTest(ctx, testClient, is)

	err = con.Configure(ctx, map[string]string{
		"aws.region":          "us-east-1",
		"aws.accessKeyId":     "accesskeymock",
		"aws.secretAccessKey": "accesssecretmock",
		"aws.url":             "http://localhost:4566",
		"streamARN":           stream1,
		"useMultiStreamMode":  "true",
	})
	is.NoErr(err)

	stream2 := setupDestinationTest(ctx, con.client, is)
	stream3 := setupDestinationTest(ctx, con.client, is)

	// wait for first stream to be created
	err = con.Open(ctx)
	is.NoErr(err)

	var recs []sdk.Record
	recs1 := testRecords(t)
	recs = append(recs, recs1...)

	recs2 := testRecordsStreamOnColField(t, stream2)
	recs = append(recs, recs2...)

	recs3 := testRecordsStreamOnColField(t, stream3)
	recs = append(recs, recs3...)

	written, err := con.Write(ctx, recs)
	is.NoErr(err)
	is.Equal(written, len(recs))

	assertWrittenRecordsOnStream(is, con.client, stream1, recs1)
	assertWrittenRecordsOnStream(is, con.client, stream2, recs2)
	assertWrittenRecordsOnStream(is, con.client, stream3, recs3)

	cleanupTest(ctx, con.client, con.config.StreamARN)

	err = con.Teardown(ctx)
	is.NoErr(err)
}

func setupDestinationTest(ctx context.Context, client *kinesis.Client, is *is.I) string {
	testID := ulid.Make()

	streamName := "stream-destination" + testID.String()
	_, err := client.CreateStream(ctx, &kinesis.CreateStreamInput{
		StreamName: &streamName,
	})
	if err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			fmt.Println("stream already exists")
		}
	} else {
		is.NoErr(err)
	}

	time.Sleep(time.Second * 1)

	describe, err := client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamName: &streamName,
	})
	is.NoErr(err)

	return *describe.StreamDescription.StreamARN
}

func cleanupTest(ctx context.Context, client *kinesis.Client, streamARN string) {
	_, err := client.DeleteStream(ctx, &kinesis.DeleteStreamInput{
		EnforceConsumerDeletion: aws.Bool(true),
		StreamARN:               &streamARN,
	})
	if err != nil {
		sdk.Logger(ctx).Err(err).Send()
	}
}

func TestTeardown_Open(t *testing.T) {
	is := is.New(t)
	logger := zerolog.New(zerolog.NewTestWriter(t))
	ctx := logger.WithContext(context.Background())
	con := Destination{
		client:     &kinesis.Client{},
		httpClient: &http.Client{},
	}

	err := con.Configure(ctx, cfg)
	is.NoErr(err)

	con.config.StreamARN = setupDestinationTest(ctx, con.client, is)
	defer cleanupTest(ctx, con.client, con.config.StreamARN)

	err = con.Open(ctx)
	is.NoErr(err)

	err = con.Teardown(ctx)
	is.NoErr(err)
}

func TestWrite_PutRecords(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t))
	ctx := logger.WithContext(context.Background())
	is := is.New(t)
	con := Destination{
		client:     &kinesis.Client{},
		httpClient: &http.Client{},
	}

	err := con.Configure(ctx, cfg)
	is.NoErr(err)

	con.config.StreamARN = setupDestinationTest(ctx, con.client, is)

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

			listShards, err := con.client.ListShards(ctx, &kinesis.ListShardsInput{
				StreamARN: &con.config.StreamARN,
			})
			is.NoErr(err)

			var recs []types.Record
			for _, shard := range listShards.Shards {
				si, err := con.client.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
					ShardId:           shard.ShardId,
					ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
					StreamARN:         &con.config.StreamARN,
				})
				is.NoErr(err)

				getRecs, err := con.client.GetRecords(ctx, &kinesis.GetRecordsInput{
					StreamARN:     &con.config.StreamARN,
					ShardIterator: si.ShardIterator,
				})
				is.NoErr(err)

				recs = append(recs, getRecs.Records...)
			}

			is.Equal(count, len(recs))
		})
	}

	cleanupTest(ctx, con.client, con.config.StreamARN)

	err = con.Teardown(ctx)
	is.NoErr(err)
}

func TestWrite_PutRecord(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t))
	ctx := logger.WithContext(context.Background())
	is := is.New(t)
	con := newDestination()

	err := con.Configure(ctx, cfg)
	is.NoErr(err)

	con.config.StreamARN = setupDestinationTest(ctx, con.client, is)

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

			listShards, err := con.client.ListShards(ctx, &kinesis.ListShardsInput{
				StreamARN: &con.config.StreamARN,
			})
			is.NoErr(err)

			var recs []types.Record
			for _, shard := range listShards.Shards {
				si, err := con.client.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
					ShardId:           shard.ShardId,
					ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
					StreamARN:         &con.config.StreamARN,
				})
				is.NoErr(err)

				getRecs, err := con.client.GetRecords(ctx, &kinesis.GetRecordsInput{
					StreamARN:     &con.config.StreamARN,
					ShardIterator: si.ShardIterator,
				})
				is.NoErr(err)

				recs = append(recs, getRecs.Records...)
			}

			is.Equal(count, len(recs))
		})
	}

	cleanupTest(ctx, con.client, con.config.StreamARN)

	err = con.Teardown(ctx)
	is.NoErr(err)
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

func assertWrittenRecordsOnStream(is *is.I, client *kinesis.Client, streamARN string, records []sdk.Record) {
	ctx := context.Background()
	listShards, err := client.ListShards(ctx, &kinesis.ListShardsInput{
		StreamARN: &streamARN,
	})
	is.NoErr(err)

	var recs []types.Record
	for _, shard := range listShards.Shards {
		si, err := client.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
			ShardId:           shard.ShardId,
			ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
			StreamARN:         &streamARN,
		})
		is.NoErr(err)

		getRecs, err := client.GetRecords(ctx, &kinesis.GetRecordsInput{
			StreamARN:     &streamARN,
			ShardIterator: si.ShardIterator,
		})
		is.NoErr(err)

		recs = append(recs, getRecs.Records...)
	}

	is.Equal(len(records), len(recs))
}
