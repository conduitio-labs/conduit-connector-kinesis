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

type streamARNParser interface {
	ParseStreamARN(sdk.Record) (string, error)
}

type fromMetadataParser struct {
	defaultStreamARN string
}

func (p *fromMetadataParser) ParseStreamARN(r sdk.Record) (string, error) {
	streamARN, err := r.Metadata.GetCollection()
	if err != nil {
		//nolint:nilerr // err is not nil if metadata is not set, so we can safely ignore the error
		return p.defaultStreamARN, nil
	}

	return streamARN, nil
}

type fromTemplateParser struct {
	template *template.Template
}

func newFromTemplateParser(templateContents string) (*fromTemplateParser, error) {
	t, err := template.New("streamARN").Parse(templateContents)
	if err != nil {
		return nil, fmt.Errorf("failed to parse streamARN template: %w", err)
	}

	return &fromTemplateParser{template: t}, nil
}

func (p *fromTemplateParser) ParseStreamARN(r sdk.Record) (string, error) {
	var sb strings.Builder
	if err := p.template.Execute(&sb, r); err != nil {
		return "", fmt.Errorf("failed to parse streamARN from template: %w", err)
	}

	return sb.String(), nil
}

type recordBatch struct {
	streamARN string
	records   []sdk.Record
}

// parseBatches parses a list of records into batches based on the streamARN
// of the records. The batches are returned in the order they were parsed.
// They are grouped in the following manner:
//   - record 1, streamARN 1
//   - record 2, streamARN 1
//   - record 3, streamARN 2
//   - record 4, streamARN 1
//   - record 5, streamARN 1
//
// Will return the following batches:
//   - batch 1: [record 1, record 2]
//   - batch 2: [record 3]
//   - batch 3: [record 4, record 5]
//
// They are parsed this way to preserve write order.
func parseBatches(records []sdk.Record, parser streamARNParser) ([]recordBatch, error) {
	var batches []recordBatch
	for _, r := range records {
		streamARN, err := parser.ParseStreamARN(r)
		if err != nil {
			return nil, fmt.Errorf("failed to parse streamARN from record: %w", err)
		}

		if len(batches) == 0 {
			batches = append(batches, recordBatch{
				streamARN: streamARN,
				records:   []sdk.Record{r},
			})
			continue
		}

		if batch := batches[len(batches)-1]; batch.streamARN == streamARN {
			batch.records = append(batch.records, r)
		} else {
			batches = append(batches, recordBatch{
				streamARN: streamARN,
				records:   []sdk.Record{r},
			})
		}
	}

	return batches, nil
}
