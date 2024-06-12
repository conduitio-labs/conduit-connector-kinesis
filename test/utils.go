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

package testutils

import (
	"context"
	"math"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/matryer/is"
)

func GetRecords(
	ctx context.Context, is *is.I,
	streamName string, client *kinesis.Client,
) []types.Record {
	streamData, err := client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamName: &streamName,
	})
	is.NoErr(err)

	listShards, err := client.ListShards(ctx, &kinesis.ListShardsInput{
		StreamARN: streamData.StreamDescription.StreamARN,
	})
	is.NoErr(err)

	var recs []types.Record
	for _, shard := range listShards.Shards {
		si, err := client.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
			ShardId:           shard.ShardId,
			ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
			StreamARN:         streamData.StreamDescription.StreamARN,
		})
		is.NoErr(err)

		getRecs, err := client.GetRecords(ctx, &kinesis.GetRecordsInput{
			StreamARN:     streamData.StreamDescription.StreamARN,
			ShardIterator: si.ShardIterator,
		})
		is.NoErr(err)

		recs = append(recs, getRecs.Records...)
	}

	return recs
}

// SetupTestStream creates a test stream and returns the name of the stream and a function to delete the stream.
func SetupTestStream(ctx context.Context, is *is.I, client *kinesis.Client) (streamName string, cleanup func()) {
	streamName = "stream_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	_, err := client.CreateStream(ctx, &kinesis.CreateStreamInput{
		StreamName: &streamName,
	})
	is.NoErr(err)

	for i := 0; i < 10; i++ {
		time.Sleep(time.Duration(100*math.Pow(2, float64(i))) * time.Millisecond)

		streamStatus, err := client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
			StreamName: &streamName,
		})
		is.NoErr(err)

		if streamStatus.StreamDescription.StreamStatus == types.StreamStatusActive {
			goto streamIsActive
		}
	}

	is.Fail() // stream not ready

streamIsActive:

	return streamName, func() {
		_, err := client.DeleteStream(ctx, &kinesis.DeleteStreamInput{
			EnforceConsumerDeletion: aws.Bool(true),
			StreamName:              &streamName,
		})
		is.NoErr(err)
	}
}

func TeardownSource(ctx context.Context, is *is.I, con sdk.Source) {
	err := con.Teardown(ctx)
	is.NoErr(err)
}

func TeardownDestination(ctx context.Context, is *is.I, con sdk.Destination) {
	err := con.Teardown(ctx)
	is.NoErr(err)
}

func GetTestConfig(streamName string) map[string]string {
	return map[string]string{
		"aws.region":          "us-east-1",
		"aws.accessKeyId":     "accesskeymock",
		"aws.secretAccessKey": "accesssecretmock",
		"aws.url":             "http://localhost:4566",
		"streamName":          streamName,
	}
}
