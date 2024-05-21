package kinesis

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/google/uuid"
	"go.uber.org/goleak"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func TestAcceptance(t *testing.T) {
	cfg := map[string]string{
		"aws.region":          "us-east-1",
		"aws.accessKeyId":     "accesskeymock",
		"aws.secretAccessKey": "accesssecretmock",
		"aws.url":             "http://localhost:4566",
	}

	testDriver := sdk.ConfigurableAcceptanceTestDriver{
		Config: sdk.ConfigurableAcceptanceTestDriverConfig{
			Connector:         Connector,
			GoleakOptions:     []goleak.Option{goleak.IgnoreCurrent()},
			SourceConfig:      cfg,
			DestinationConfig: cfg,
			GenerateDataType:  sdk.GenerateRawData,
			BeforeTest: func(t *testing.T) {
				setRandomStreamARNToCfg(t, cfg)
			},
			Skip: []string{
				"TestDestination_Configure_RequiredParams",
				"TestSource_Configure_RequiredParams",

				"TestDestination_Configure_Success",
				"TestDestination_Parameters_Success",
				// "TestDestination_Write_Success",
				"TestSource_Configure_Success",

				"TestSource_Open_ResumeAtPositionCDC",

				"TestSource_Open_ResumeAtPositionSnapshot",
				"TestSource_Parameters_Success",
				"TestSource_Read_Success",
				"TestSource_Read_Timeout",
				"TestSpecifier_Exists",
				"TestSpecifier_Specify_Success",
			},
			WriteTimeout: 500 * time.Millisecond,
			ReadTimeout:  3000 * time.Millisecond,
		},
	}

	sdk.AcceptanceTest(t, testDriver)
}

func TestNewRandomStreamDoesntLeak(t *testing.T) {
	// While testing a test function seems redundant, it is useful here to
	// discard goroutine leak origins.

	defer goleak.VerifyNone(t)
	cfg := map[string]string{
		"aws.region":          "us-east-1",
		"aws.accessKeyId":     "accesskeymock",
		"aws.secretAccessKey": "accesssecretmock",
		"aws.url":             "http://localhost:4566",
	}

	setRandomStreamARNToCfg(t, cfg)
	t.Log(cfg["streamARN"])
}

func setRandomStreamARNToCfg(t *testing.T, cfg map[string]string) {
	is := is.New(t)

	ctx := context.Background()

	httpClient := &http.Client{}
	defer httpClient.CloseIdleConnections()

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
		config.WithHTTPClient(httpClient),
	)
	is.NoErr(err)

	streamName := "acceptance_" + uuid.NewString()[0:8]
	client := kinesis.NewFromConfig(awsCfg)

	// Acceptance tests are susceptible to wrong record order. By default
	// kinesis writes to multiple shards, making difficult debugging whether the
	// read order of records is bad because of bad implementation or randomness.
	// Forcing the shard count to 1 simplifies the issue.
	var count int32 = 1
	_, err = client.CreateStream(ctx, &kinesis.CreateStreamInput{
		StreamName: &streamName,
		ShardCount: &count,
	})
	is.NoErr(err)

	for {
		describe, err := client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
			StreamName: &streamName,
		})
		is.NoErr(err)

		isStreamReadyForTest := describe.StreamDescription.StreamStatus == types.StreamStatusActive
		if isStreamReadyForTest {
			cfg["streamARN"] = *describe.StreamDescription.StreamARN
			break
		}

		time.Sleep(100 * time.Millisecond)
	}
}
