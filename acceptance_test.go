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
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/conduitio-labs/conduit-connector-kinesis/common"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/matryer/is"
	"go.uber.org/goleak"
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
			},
			WriteTimeout: 3000 * time.Millisecond,
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
		config.WithHTTPClient(httpClient),
	)
	is.NoErr(err)

	streamName := "acceptance_" + uuid.NewString()[0:8]
	resolver, err := common.NewEndpointResolver("http://localhost:4566")
	is.NoErr(err)

	client := kinesis.NewFromConfig(awsCfg, kinesis.WithEndpointResolverV2(resolver))

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
