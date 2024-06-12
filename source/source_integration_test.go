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

package source

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	testutils "github.com/conduitio-labs/conduit-connector-kinesis/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	cmap "github.com/orcaman/concurrent-map/v2"
)

func TestRead(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	con := &Source{
		httpClient: &http.Client{Transport: &http.Transport{}},
		buffer:     make(chan sdk.Record, 1),
		streamMap:  cmap.New[*kinesis.SubscribeToShardEventStream](),
	}

	streamName, cleanup := testutils.SetupTestStream(ctx, is, con.client)
	defer cleanup()

	err := con.Configure(ctx, testutils.GetTestConfig(streamName))
	is.NoErr(err)

	recs := testutils.GetRecords(ctx, is, con.config.StreamName, con.client)

	t.Logf("%v read records using getRecords", len(recs))

	err = con.Open(ctx, nil)
	is.NoErr(err)

	defer testutils.TeardownSource(ctx, is, con)

	for i := 0; i < 5; i++ {
		_, err := con.Read(ctx)
		is.NoErr(err)
	}

	t.Log("records read")

	putRecResp, err := con.client.PutRecord(ctx, &kinesis.PutRecordInput{
		StreamName:   &con.config.StreamName,
		Data:         []byte("some data here"),
		PartitionKey: aws.String("5"),
	})
	is.NoErr(err)

	t.Log(putRecResp.ShardId, putRecResp.SequenceNumber)

	sequenceNumber := *putRecResp.SequenceNumber

	var readRec sdk.Record
	for {
		rec, err := con.Read(ctx)
		if errors.Is(err, sdk.ErrBackoffRetry) {
			continue
		}

		is.NoErr(err)
		readRec = rec

		break
	}

	is.Equal("kinesis-"+sequenceNumber, readRec.Metadata["sequenceNumber"])

	// expect message to be read from subscription before timeout
	for {
		// send value and then block read until it comes in
		go func() {
			_, err := con.client.PutRecord(ctx, &kinesis.PutRecordInput{
				StreamName:   &con.config.StreamName,
				Data:         []byte("some data here again"),
				PartitionKey: aws.String("6"),
			})
			is.NoErr(err)
		}()

		_, err := con.Read(ctx)
		if errors.Is(err, sdk.ErrBackoffRetry) {
			continue
		}

		is.NoErr(err)

		break
	}
}
