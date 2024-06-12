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
	"strings"
	"text/template"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/conduitio-labs/conduit-connector-kinesis/common"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Destination struct {
	sdk.UnimplementedDestination

	config Config

	// client is the Client for the AWS Kinesis API
	client *kinesis.Client

	recordWriter recordWriter

	// partitionKeyTempl is the parsed template given from the
	// PartitionKeyTemplate configuration parameter. If none given
	// will be set to nil.
	partitionKeyTempl *template.Template

	// httpClient is the http.Client used for interacting with the kinesis API.
	// We need a custom one so that we can cleanup leaking http connections on
	// the teardown method.
	httpClient *http.Client

	// streamNameParser is the parser used to parse the streamName from a record.
	streamNameParser streamNameParser
}

func newDestination() *Destination {
	httpClient := &http.Client{Transport: &http.Transport{}}
	return &Destination{httpClient: httpClient}
}

func New() sdk.Destination {
	return sdk.DestinationWithMiddleware(newDestination(), sdk.DefaultDestinationMiddleware()...)
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

	if d.config.PartitionKeyTemplate != "" {
		d.partitionKeyTempl, err = template.New("partitionKey").Parse(d.config.PartitionKeyTemplate)
		if err != nil {
			return fmt.Errorf("error parsing partition key template: %w", err)
		}
	}

	if isGoTemplate(d.config.StreamName) {
		d.recordWriter = &multiStreamWriter{destination: d}

		d.streamNameParser, err = newFromTemplateParser(d.config.StreamName)
		if err != nil {
			return fmt.Errorf("failed to create streamName parser: %w", err)
		}
	} else if d.config.StreamName == "" {
		d.recordWriter = &multiStreamWriter{destination: d}

		// the default behaviour is to get the streamName from the
		// opencdc.collection metadata field
		d.streamNameParser = &fromColFieldParser{defaultStreamName: d.config.StreamName}

	} else {
		d.recordWriter = &singleStreamWriter{destination: d}
	}

	d.client, err = common.NewClient(ctx, d.httpClient, d.config.Config)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	return nil
}

func (d *Destination) Open(ctx context.Context) error {
	// DescribeStream to know that the stream ARN is valid and usable, ie test connection
	_, err := d.client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamName: &d.config.StreamName,
	})
	if err != nil {
		return fmt.Errorf("error when attempting to test connection to stream: %w", err)
	}

	sdk.Logger(ctx).Info().Msg("destination ready to be written to")

	return nil
}

func (d *Destination) partitionKey(ctx context.Context, rec sdk.Record) (string, error) {
	if d.config.PartitionKeyTemplate == "" {
		partitionKey := string(rec.Key.Bytes())
		// partition keys must be less than 256 characters
		// https://docs.aws.amazon.com/streams/latest/dev/key-concepts.html#partition-key
		if len(partitionKey) > 256 {
			partitionKey = partitionKey[:256]
			sdk.Logger(ctx).Warn().
				Msg("detected record key greater than 256 characters as a partition key; trimming it down")
		}

		return partitionKey, nil
	}

	var sb strings.Builder
	if err := d.partitionKeyTempl.Execute(&sb, rec); err != nil {
		return "", fmt.Errorf("failed to create partition key from template: %w", err)
	}

	return sb.String(), nil
}

func (d *Destination) createPutRequestInput(
	ctx context.Context,
	records []sdk.Record,
	streamName string,
) (*kinesis.PutRecordsInput, error) {
	entries := make([]types.PutRecordsRequestEntry, 0, len(records))

	for _, rec := range records {
		partitionKey, err := d.partitionKey(ctx, rec)
		if err != nil {
			return nil, err
		}

		recordEntry := types.PutRecordsRequestEntry{
			Data:         rec.Bytes(),
			PartitionKey: &partitionKey,
		}
		entries = append(entries, recordEntry)
	}

	return &kinesis.PutRecordsInput{
		StreamName: &streamName,
		Records:    entries,
	}, nil
}

func (d *Destination) Write(ctx context.Context, records []sdk.Record) (written int, err error) {
	if written, err = d.recordWriter.Write(ctx, records); err != nil {
		return written, fmt.Errorf("failed to write records: %w", err)
	}

	return written, nil
}

func (d *Destination) Teardown(ctx context.Context) error {
	d.httpClient.CloseIdleConnections()
	sdk.Logger(ctx).Info().Msg("closed all httpClient unused connections")
	return nil
}

type recordWriter interface {
	Write(ctx context.Context, records []sdk.Record) (int, error)
}

type singleStreamWriter struct {
	destination *Destination
}

func (s *singleStreamWriter) Write(ctx context.Context, records []sdk.Record) (int, error) {
	req, err := s.destination.createPutRequestInput(ctx, records, s.destination.config.StreamName)
	if err != nil {
		return 0, fmt.Errorf("failed to create put records request: %w", err)
	}

	var written int
	output, err := s.destination.client.PutRecords(ctx, req)
	if err != nil {
		return written, fmt.Errorf("failed to put %v records into %s: %w", len(req.Records), *req.StreamName, err)
	}

	for _, rec := range output.Records {
		if rec.ErrorCode != nil {
			return written, fmt.Errorf(
				"error when attempting to insert record, error code %s: %s",
				*rec.ErrorCode, *rec.ErrorMessage,
			)
		}
		written++
	}

	sdk.Logger(ctx).Debug().Msgf("wrote %s records to destination", strconv.Itoa(written))
	return written, nil
}

type multiStreamWriter struct {
	destination *Destination
}

func (m *multiStreamWriter) Write(ctx context.Context, records []sdk.Record) (int, error) {
	batches, err := parseBatches(records, m.destination.streamNameParser)
	if err != nil {
		return 0, fmt.Errorf("failed to parse batches: %w", err)
	}

	var written int
	for _, batch := range batches {
		req, err := m.destination.createPutRequestInput(ctx, batch.records, batch.streamName)
		if err != nil {
			return written, fmt.Errorf("failed to create put records request: %w", err)
		}

		output, err := m.destination.client.PutRecords(ctx, req)
		if err != nil {
			return written, fmt.Errorf("failed to put %v records: %w", len(req.Records), err)
		}

		for _, rec := range output.Records {
			if rec.ErrorCode != nil {
				return written, fmt.Errorf(
					"error when attempting to insert record, error code %s: %s",
					*rec.ErrorCode, *rec.ErrorMessage,
				)
			}
			written++
		}
	}

	sdk.Logger(ctx).Debug().Msgf("wrote %s records to destination", strconv.Itoa(written))
	return written, nil
}

func isGoTemplate(template string) bool {
	return strings.HasPrefix(template, "{{") && strings.HasSuffix(template, "}}")
}
