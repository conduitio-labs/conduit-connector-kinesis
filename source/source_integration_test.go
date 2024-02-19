package source

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

var cfg map[string]string = map[string]string{
	"streamARN":           "arn:aws:kinesis:us-east-1:000000000000:stream/stream1",
	"aws.region":          "us-east-1",
	"aws.accessKeyId":     "accesskeymock",
	"aws.secretAccessKey": "accesssecretmock",
}

func LocalKinesisClient(ctx context.Context, srcConfig Config, is *is.I) *kinesis.Client {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(srcConfig.AWSRegion),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				srcConfig.AWSAccessKeyID,
				srcConfig.AWSSecretAccessKey,
				"")),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(_, _ string, _ ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				PartitionID:       "aws",
				URL:               "http://localhost:4566",
				SigningRegion:     srcConfig.AWSRegion,
				HostnameImmutable: true,
			}, nil
		})),
	)
	is.NoErr(err)

	return kinesis.NewFromConfig(cfg)
}

func TestTeardown_Open(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	con := Source{}

	err := con.Configure(ctx, cfg)
	is.NoErr(err)

	con.client = LocalKinesisClient(ctx, con.config, is)

	err = con.Open(ctx, nil)
	is.NoErr(err)

	err = con.Teardown(ctx)
	is.NoErr(err)
}

func TestRead(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	con := Source{
		buffer: make(chan sdk.Record, 1),
		ticker: *time.NewTicker(time.Second * 10),
	}

	err := con.Configure(ctx, cfg)
	is.NoErr(err)

	con.client = LocalKinesisClient(ctx, con.config, is)
	con.config.StreamARN = setupSourceTest(ctx, con.client, is)

	defer func() {
		err := con.Teardown(ctx)
		is.NoErr(err)

		cleanupTest(ctx, con.client, con.config.StreamARN)
	}()

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

	fmt.Println(recs)

	length := len(recs)
	fmt.Println(length, "read records using getRecords")

	err = con.Open(ctx, nil)
	is.NoErr(err)

	for i := 0; i < 5; i++ {
		rec, err := con.Read(ctx)
		is.NoErr(err)

		fmt.Println(rec)
	}

	_, err = con.Read(ctx)
	is.Equal(err, sdk.ErrBackoffRetry)

	// send a new message
	putRecResp, err := con.client.PutRecord(ctx, &kinesis.PutRecordInput{
		StreamARN:    &con.config.StreamARN,
		Data:         []byte("some data here"),
		PartitionKey: aws.String("5"),
	})
	is.NoErr(err)

	rec, err := con.Read(ctx)
	is.NoErr(err)

	fmt.Println(rec)
	is.Equal(*putRecResp.SequenceNumber, rec.Metadata["sequenceNumber"])
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
		fmt.Println("failed to delete stream")
	}
}
