package kinesis

//go:generate paramgen -output=paramgen_dest.go DestinationConfig

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Destination struct {
	sdk.UnimplementedDestination

	config DestinationConfig

	// KinesisClient is the Client for the AWS Kinesis API
	KinesisClient *kinesis.Client
}

type DestinationConfig struct {
	// Config includes parameters that are the same in the source and destination.
	Config
}

func NewDestination() sdk.Destination {
	// Create Destination and wrap it in the default middleware.
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
}

func (d *Destination) Parameters() map[string]sdk.Parameter {
	// Parameters is a map of named Parameters that describe how to configure
	// the Destination. Parameters can be generated from DestinationConfig with
	// paramgen.
	return d.config.Parameters()
}

func (d *Destination) Configure(ctx context.Context, cfg map[string]string) error {
	// Configure is the first function to be called in a connector. It provides
	// the connector with the configuration that can be validated and stored.
	// In case the configuration is not valid it should return an error.
	// Testing if your connector can reach the configured data source should be
	// done in Open, not in Configure.
	// The SDK will validate the configuration and populate default values
	// before calling Configure. If you need to do more complex validations you
	// can do them manually here.

	sdk.Logger(ctx).Info().Msg("Configuring Destination...")
	err := sdk.Util.ParseConfig(cfg, &d.config)
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	d.KinesisClient = kinesis.New(kinesis.Options{
		Region: d.config.AWSRegion,
	})

	return nil
}

func (d *Destination) Open(ctx context.Context) error {
	// Open is called after Configure to signal the plugin it can prepare to
	// start writing records. If needed, the plugin should open connections in
	// this function.
	stream, err := d.KinesisClient.DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamARN: &d.config.StreamARN,
	})
	if err != nil {
		sdk.Logger(ctx).Error().Msg("error when attempting to describe stream")
		return err
	}

	d.config.StreamName = *stream.StreamDescription.StreamName

	return nil
}

func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	// Write writes len(r) records from r to the destination right away without
	// caching. It should return the number of records written from r
	// (0 <= n <= len(r)) and any error encountered that caused the write to
	// stop early. Write must return a non-nil error if it returns n < len(r).

	// Kinesis put records requests have a size limit of 5 MB per request, 1 MB per record,
	// and a count limit of 500 records, so generate the records requests first

	d.KinesisClient.PutRecords(ctx, &kinesis.PutRecordsInput{
		StreamARN: &d.config.StreamARN,
		Records:   []types.PutRecordsRequestEntry{},
	})
	return 0, nil
}

func (d *Destination) Teardown(ctx context.Context) error {
	// Teardown signals to the plugin that all records were written and there
	// will be no more calls to any other function. After Teardown returns, the
	// plugin should be ready for a graceful shutdown.
	return nil
}
