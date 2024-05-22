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

package kinesis

import (
	"context"
	"testing"

	"github.com/matryer/is"
	"github.com/mer-oscar/conduit-connector-kinesis/destination"
	"github.com/mer-oscar/conduit-connector-kinesis/source"
	"go.uber.org/goleak"
)

func TestConnectorCleanup(t *testing.T) {
	defer goleak.VerifyNone(t)

	is := is.New(t)
	ctx := context.Background()

	cfg := map[string]string{
		"aws.region":          "us-east-1",
		"aws.accessKeyId":     "accesskeymock",
		"aws.secretAccessKey": "accesssecretmock",
		"aws.url":             "http://localhost:4566",
	}

	setRandomStreamARNToCfg(t, cfg)

	src := source.New()
	err := src.Configure(ctx, cfg)
	is.NoErr(err)

	err = src.Open(ctx, nil)
	is.NoErr(err)

	err = src.Teardown(ctx)
	is.NoErr(err)

	dest := destination.New()
	err = dest.Configure(ctx, cfg)
	is.NoErr(err)

	err = dest.Open(ctx)
	is.NoErr(err)

	err = dest.Teardown(ctx)
	is.NoErr(err)
}
