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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/conduitio-labs/conduit-connector-kinesis/common"
	testutils "github.com/conduitio-labs/conduit-connector-kinesis/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/matryer/is"
	"go.uber.org/goleak"
)

func TestAcceptance(t *testing.T) {
	cfg := testutils.GetTestConfig("") // streamName is set in testutils.SetupTestStream

	testDriver := sdk.ConfigurableAcceptanceTestDriver{
		Config: sdk.ConfigurableAcceptanceTestDriverConfig{
			Connector:         Connector,
			GoleakOptions:     []goleak.Option{goleak.IgnoreCurrent()},
			SourceConfig:      cfg,
			DestinationConfig: cfg,
			GenerateDataType:  sdk.GenerateRawData,
			BeforeTest: func(t *testing.T) {
				setRandomStreamNameToCfg(t, cfg)
			},
			AfterTest: func(t *testing.T) {
				cleanupAcceptanceTestStream(t, cfg)
			},
			Skip: []string{
				"TestDestination_Configure_RequiredParams",
				"TestSource_Configure_RequiredParams",
			},
			WriteTimeout: 5000 * time.Millisecond,
			ReadTimeout:  5000 * time.Millisecond,
		},
	}

	sdk.AcceptanceTest(t, testDriver)
}

func TestNewRandomStreamDoesntLeak(t *testing.T) {
	// While testing a test function seems redundant, it is useful here to
	// discard goroutine leak origins.

	defer goleak.VerifyNone(t)
	cfg := testutils.GetTestConfig("")

	setRandomStreamNameToCfg(t, cfg)
}

func setRandomStreamNameToCfg(t *testing.T, cfg map[string]string) {
	is := is.New(t)

	ctx := context.Background()

	httpClient := &http.Client{}
	defer httpClient.CloseIdleConnections()

	streamName := "acceptance_" + uuid.NewString()[0:8]
	client, err := common.NewClient(ctx, httpClient, common.Config{
		AWSAccessKeyID:     cfg["aws.accessKeyId"],
		AWSSecretAccessKey: cfg["aws.secretAccessKey"],
		AWSRegion:          cfg["aws.region"],
		AWSURL:             "http://localhost:4566",
	})
	is.NoErr(err)

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
			cfg["streamName"] = *describe.StreamDescription.StreamName
			break
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func cleanupAcceptanceTestStream(t *testing.T, cfg map[string]string) {
	is := is.New(t)

	ctx := context.Background()

	httpClient := &http.Client{}
	defer httpClient.CloseIdleConnections()

	client, err := common.NewClient(ctx, httpClient, common.Config{
		AWSAccessKeyID:     cfg["aws.accessKeyId"],
		AWSSecretAccessKey: cfg["aws.secretAccessKey"],
		AWSRegion:          cfg["aws.region"],
		AWSURL:             "http://localhost:4566",
	})
	is.NoErr(err)

	describe, err := client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamName: aws.String(cfg["streamName"]),
	})
	is.NoErr(err)

	_, err = client.DeleteStream(ctx, &kinesis.DeleteStreamInput{
		EnforceConsumerDeletion: aws.Bool(true),
		StreamARN:               describe.StreamDescription.StreamARN,
	})
	is.NoErr(err)
}
