package destination

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
)

const (
	defaultBatchSize = 500
)

type Destination struct {
	sdk.UnimplementedDestination

	config Config

	// client is the Client for the AWS Kinesis API
	client *kinesis.Client
}

// NewDestination creates a Destination and wrap it in the default middleware.
func NewDestination() sdk.Destination {
	middlewares := sdk.DefaultDestinationMiddleware()
	for i, m := range middlewares {
		switch dest := m.(type) {
		case sdk.DestinationWithBatch:
			dest.DefaultBatchSize = defaultBatchSize
			middlewares[i] = dest
		default:
		}
	}

	return sdk.DestinationWithMiddleware(&Destination{}, middlewares...)
}

func (d *Destination) Parameters() map[string]sdk.Parameter {
	return Config{}.Parameters()
}

func (d *Destination) Configure(ctx context.Context, cfg map[string]string) error {
	sdk.Logger(ctx).Info().Msg("Configuring Destination...")
	err := sdk.Util.ParseConfig(cfg, &d.config)
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	// Configure the creds for the client
	var cfgOptions []func(*config.LoadOptions) error
	cfgOptions = append(cfgOptions, config.WithRegion(d.config.AWSRegion))
	cfgOptions = append(cfgOptions, config.WithCredentialsProvider(
		credentials.NewStaticCredentialsProvider(
			d.config.AWSAccessKeyID,
			d.config.AWSSecretAccessKey,
			"")))

	if d.config.AWSURL != "" {
		cfgOptions = append(cfgOptions, config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(_, _ string, _ ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				PartitionID:       "aws",
				URL:               d.config.AWSURL,
				SigningRegion:     d.config.AWSRegion,
				HostnameImmutable: true,
			}, nil
		},
		)))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx,
		cfgOptions...,
	)
	if err != nil {
		return fmt.Errorf("failed to load aws config with given credentials : %w", err)
	}

	d.client = kinesis.NewFromConfig(awsCfg)

	return nil
}

func (d *Destination) Open(ctx context.Context) error {
	// DescribeStream to know that the stream ARN is valid and usable, ie test connection
	_, err := d.client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamARN: &d.config.StreamARN,
	})
	if err != nil {
		sdk.Logger(ctx).Error().Msg("error when attempting to test connection to stream")
		return err
	}

	return nil
}

func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	if d.config.UseSingleShard {
		partition := uuid.New().String()
		var count int
		for _, record := range records {
			_, err := d.client.PutRecord(ctx, &kinesis.PutRecordInput{
				PartitionKey: &partition,
				Data:         record.Bytes(),
				StreamARN:    &d.config.StreamARN,
			})
			if err != nil {
				return count, err
			}

			count++
		}

		return count, nil
	}

	var entries []types.PutRecordsRequestEntry
	var req *kinesis.PutRecordsInput

	// create the put records request
	for j := 0; j < len(records); j++ {
		pKey := string(records[j].Key.Bytes())
		if len(pKey) > 256 {
			pKey = pKey[:256]
		}

		recordEntry := types.PutRecordsRequestEntry{
			Data:         records[j].Bytes(),
			PartitionKey: &pKey,
		}
		entries = append(entries, recordEntry)
	}

	req = &kinesis.PutRecordsInput{
		StreamARN:  &d.config.StreamARN,
		StreamName: &d.config.StreamName,
		Records:    entries,
	}

	var written int
	output, err := d.client.PutRecords(ctx, req)
	if err != nil {
		return written, err
	}

	for _, rec := range output.Records {
		if rec.ErrorCode != nil {
			sdk.Logger(ctx).Error().Msg("error when attempting to insert record: " + *rec.ErrorCode + " " + *rec.ErrorMessage)
			continue
		}
		written++
	}

	sdk.Logger(ctx).Debug().Msg("wrote " + strconv.Itoa(written) + " records to destination")
	return written, nil
}

func (d *Destination) Teardown(ctx context.Context) error {
	// no shutdown required
	return nil
}
