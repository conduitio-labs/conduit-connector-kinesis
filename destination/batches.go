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
	"fmt"
	"strings"
	"text/template"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

type streamNameParser interface {
	ParseStreamName(sdk.Record) (string, error)
}

// fromColFieldParser parses the streamName from the opencdc.collection record
// metadata field.
type fromColFieldParser struct{}

func (p *fromColFieldParser) ParseStreamName(r sdk.Record) (string, error) {
	streamName, err := r.Metadata.GetCollection()
	if err != nil {
		//nolint:nilerr // err is not nil if metadata is not set, so we can safely ignore the error
		return "", nil
	}

	return streamName, nil
}

// fromTemplateParser parses the streamName from the given go template.
type fromTemplateParser struct {
	template *template.Template
}

func newFromTemplateParser(templateContents string) (*fromTemplateParser, error) {
	t, err := template.New("streamName").Parse(templateContents)
	if err != nil {
		return nil, fmt.Errorf("failed to parse streamName template: %w", err)
	}

	return &fromTemplateParser{template: t}, nil
}

func (p *fromTemplateParser) ParseStreamName(r sdk.Record) (string, error) {
	var sb strings.Builder
	if err := p.template.Execute(&sb, r); err != nil {
		return "", fmt.Errorf("failed to parse streamName from template: %w", err)
	}

	return sb.String(), nil
}

type recordBatch struct {
	streamName string
	records    []sdk.Record
}

// parseBatches parses a list of records into batches based on the streamName
// of the records. The batches are returned in the order they were parsed.
// They are grouped in the following manner:
//   - record 1, streamName 1
//   - record 2, streamName 1
//   - record 3, streamName 2
//   - record 4, streamName 1
//   - record 5, streamName 1
//
// Will return the following batches:
//   - batch 1: [record 1, record 2]
//   - batch 2: [record 3]
//   - batch 3: [record 4, record 5]
//
// They are parsed this way to preserve write order.
func parseBatches(records []sdk.Record, parser streamNameParser) ([]recordBatch, error) {
	var batches []recordBatch
	for _, r := range records {
		streamName, err := parser.ParseStreamName(r)
		if err != nil {
			return nil, fmt.Errorf("failed to parse streamName from record: %w", err)
		}

		if len(batches) == 0 {
			batches = append(batches, recordBatch{
				streamName: streamName,
				records:    []sdk.Record{r},
			})
			continue
		}

		if batchRef := &batches[len(batches)-1]; batchRef.streamName == streamName {
			batchRef.records = append(batchRef.records, r)
		} else {
			batches = append(batches, recordBatch{
				streamName: streamName,
				records:    []sdk.Record{r},
			})
		}
	}

	return batches, nil
}
