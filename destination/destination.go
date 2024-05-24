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
	"fmt"
	"net/http"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Destination struct {
	sdk.UnimplementedDestination

	config Config

	// client is the Client for the AWS Kinesis API
	client *kinesis.Client

	// httpClient is the http.Client used for interacting with the kinesis API.
	// We need a custom one so that we can cleanup leaking http connections on
	// the teardown method.
	httpClient *http.Client
}

func New() sdk.Destination {
	httpClient := &http.Client{Transport: &http.Transport{}}
	return sdk.DestinationWithMiddleware(
		&Destination{httpClient: httpClient},
		sdk.DefaultDestinationMiddleware()...,
	)
}

func (d *Destination) Parameters() map[string]sdk.Parameter {
	return d.config.Parameters()
}

func (d *Destination) Configure(ctx context.Context, cfg map[string]string) error {
	err := sdk.Util.ParseConfig(cfg, &d.config)
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	sdk.Logger(ctx).Info().Msg("parsed destination configuration")

	// Configure the creds for the client
	var cfgOptions []func(*config.LoadOptions) error
	cfgOptions = append(cfgOptions, config.WithRegion(d.config.AWSRegion))
	cfgOptions = append(cfgOptions, config.WithCredentialsProvider(
		credentials.NewStaticCredentialsProvider(
			d.config.AWSAccessKeyID,
			d.config.AWSSecretAccessKey,
			"")))
	cfgOptions = append(cfgOptions, config.WithHTTPClient(d.httpClient))

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
	sdk.Logger(ctx).Info().Msg("loaded destination aws configuration")

	d.client = kinesis.NewFromConfig(awsCfg)

	return nil
}

func (d *Destination) Open(ctx context.Context) error {
	// DescribeStream to know that the stream ARN is valid and usable, ie test connection
	_, err := d.client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamARN: &d.config.StreamARN,
	})
	if err != nil {
		return fmt.Errorf("error when attempting to test connection to stream: %w", err)
	}

	sdk.Logger(ctx).Info().Msg("destination ready to be written to")

	return nil
}

func (d *Destination) createPutRequestInput(records []sdk.Record) *kinesis.PutRecordsInput {
	entries := make([]types.PutRecordsRequestEntry, 0, len(records))

	for _, rec := range records {
		partitionKey := d.config.PartitionKey
		if partitionKey == "" {
			partitionKey := string(rec.Key.Bytes())
			if len(partitionKey) > 256 {
				partitionKey = partitionKey[:256]
			}
		}

		recordEntry := types.PutRecordsRequestEntry{
			Data:         rec.Bytes(),
			PartitionKey: &partitionKey,
		}
		entries = append(entries, recordEntry)
	}

	return &kinesis.PutRecordsInput{
		StreamARN:  &d.config.StreamARN,
		StreamName: &d.config.StreamName,
		Records:    entries,
	}
}

func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	req := d.createPutRequestInput(records)

	var written int
	output, err := d.client.PutRecords(ctx, req)
	if err != nil {
		return written, fmt.Errorf("failed to put %v records into %s: %w", len(req.Records), *req.StreamName, err)
	}

	for _, rec := range output.Records {
		if rec.ErrorCode != nil {
			return written, fmt.Errorf("error when attempting to insert record %s: %s", *rec.ErrorCode, *rec.ErrorMessage)
		}
		written++
	}

	sdk.Logger(ctx).Debug().Msgf("wrote %s records to destination", strconv.Itoa(written))
	return written, nil
}

func (d *Destination) Teardown(ctx context.Context) error {
	d.httpClient.CloseIdleConnections()
	sdk.Logger(ctx).Info().Msg("closed all httpClient unused connections")
	return nil
}
