package kinesis

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/google/uuid"
	"go.uber.org/goleak"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func TestAcceptance(t *testing.T) {
	cfg := map[string]string{
		"streamARN":           "arn:aws:kinesis:us-east-1:000000000000:stream/stream1",
		"aws.region":          "us-east-1",
		"aws.accessKeyId":     "accesskeymock",
		"aws.secretAccessKey": "accesssecretmock",
		"aws.url":             "http://localhost:4566",
	}
	ctx := context.Background()
	is := is.New(t)

	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(cfg["aws.region"]),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				cfg["aws.accessKeyId"],
				cfg["aws.secretAccessKey"],
				"")),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(_, _ string, _ ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				PartitionID:       "aws",
				URL:               "http://localhost:4566",
				SigningRegion:     cfg["aws.region"],
				HostnameImmutable: true,
			}, nil
		})),
	)
	is.NoErr(err)

	client := kinesis.NewFromConfig(awsCfg)

	sdk.AcceptanceTest(t, sdk.ConfigurableAcceptanceTestDriver{
		Config: sdk.ConfigurableAcceptanceTestDriverConfig{
			Connector: Connector,
			GoleakOptions: []goleak.Option{
				goleak.IgnoreCurrent(),
				goleak.IgnoreAnyFunction("internal/poll.runtime_pollWait"),
				goleak.IgnoreAnyFunction("net/http.(*persistConn).writeLoop"),
			},
			SourceConfig:      cfg,
			DestinationConfig: cfg,
			GenerateDataType:  sdk.GenerateRawData,
			BeforeTest: func(t *testing.T) {
				streamName := "acceptance_" + uuid.NewString()[0:8]

				_, err = client.CreateStream(ctx, &kinesis.CreateStreamInput{
					StreamName: &streamName,
				})
				is.NoErr(err)

				time.Sleep(time.Second)

				describe, err := client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
					StreamName: &streamName,
				})
				is.NoErr(err)

				cfg["streamARN"] = *describe.StreamDescription.StreamARN
			},
			Skip: []string{
				"TestDestination_Configure_RequiredParams",
				"TestSource_Configure_RequiredParams",
			},
			WriteTimeout: 500 * time.Millisecond,
			ReadTimeout:  3000 * time.Millisecond,
		},
	})
}
