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

	"github.com/conduitio-labs/conduit-connector-kinesis/destination"
	"github.com/conduitio-labs/conduit-connector-kinesis/source"
	testutils "github.com/conduitio-labs/conduit-connector-kinesis/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	"go.uber.org/goleak"
)

func TestConnectorCleanup(t *testing.T) {
	// We make sure here that both the source and destination connectors don't leak
	// any resources when opened and closed down. It is easier this way to detect
	// possible goroutine leaks.
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	is := is.New(t)
	ctx := context.Background()

	cfg := testutils.GetTestConfig("test-source")

	setRandomStreamNameToCfg(t, cfg)

	// We do this because current sdk.Util.ParseConfig implementation
	// mutates the original passed config map.
	sourceCfg, destCfg := map[string]string{}, map[string]string{}
	for key, val := range cfg {
		sourceCfg[key] = val
		destCfg[key] = val
	}

	src := source.New()

	spec := Connector.NewSpecification()
	err := sdk.Util.ParseConfig(ctx, sourceCfg, src.Config(), spec.SourceParams)
	is.NoErr(err)

	is.NoErr(src.Open(ctx, nil))
	is.NoErr(src.Teardown(ctx))

	dest := destination.New()

	err = sdk.Util.ParseConfig(ctx, destCfg, dest.Config(), spec.DestinationParams)
	is.NoErr(err)

	is.NoErr(dest.Open(ctx))
	is.NoErr(dest.Teardown(ctx))
}
