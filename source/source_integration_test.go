package source

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	cmap "github.com/orcaman/concurrent-map/v2"
)

var cfg map[string]string = map[string]string{
	"streamARN":           "arn:aws:kinesis:us-east-1:000000000000:stream/stream-source",
	"aws.region":          "us-east-1",
	"aws.accessKeyId":     "accesskeymock",
	"aws.secretAccessKey": "accesssecretmock",
	"aws.url":             "http://localhost:4566",
}

func TestRead(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	con := Source{
		httpClient: &http.Client{Transport: &http.Transport{}},
		buffer:     make(chan sdk.Record, 1),
		streamMap:  cmap.New[*kinesis.SubscribeToShardEventStream](),
	}

	err := con.Configure(ctx, cfg)
	is.NoErr(err)

	defer func() {
		cleanupTest(ctx, con.client, con.config.StreamARN)
		con.consumerARN = nil

		err := con.Teardown(ctx)
		is.NoErr(err)
	}()

	con.config.StreamARN = setupSourceTest(ctx, con.client, is)

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

	t.Logf("%v read records using getRecords", len(recs))

	err = con.Open(ctx, nil)
	is.NoErr(err)

	for i := 0; i < 5; i++ {
		_, err := con.Read(ctx)
		is.NoErr(err)
	}

	t.Log("records read")

	putRecResp, err := con.client.PutRecord(ctx, &kinesis.PutRecordInput{
		StreamARN:    &con.config.StreamARN,
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
				StreamARN:    &con.config.StreamARN,
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

func setupSourceTest(ctx context.Context, client *kinesis.Client, is *is.I) string {
	streamName := "stream-source"
	// create stream
	_, err := client.CreateStream(ctx, &kinesis.CreateStreamInput{
		StreamName: &streamName,
	})
	is.NoErr(err)

	time.Sleep(time.Second * 5)
	describe, err := client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamName: &streamName,
	})
	is.NoErr(err)

	fmt.Println("length of shards is: ", len(describe.StreamDescription.Shards))

	var recs []types.PutRecordsRequestEntry
	for i := 0; i < 5; i++ {
		kRec := types.PutRecordsRequestEntry{
			PartitionKey: aws.String(strconv.Itoa(i)),
			Data:         []byte(fmt.Sprintf("%d - some data here", i)),
		}

		recs = append(recs, kRec)
	}

	// push some messages to it
	_, err = client.PutRecords(ctx, &kinesis.PutRecordsInput{
		StreamName: &streamName,
		Records:    recs,
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
		fmt.Println("failed to delete stream: ", streamARN)
	}
}
