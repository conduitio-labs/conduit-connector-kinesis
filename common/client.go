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

package common

import (
	"context"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func NewClient(ctx context.Context, httpClient *http.Client, cfg Config) (*kinesis.Client, error) {
	var cfgOptions []func(*config.LoadOptions) error
	cfgOptions = append(cfgOptions, config.WithRegion(cfg.AWSRegion))
	cfgOptions = append(cfgOptions, config.WithCredentialsProvider(
		credentials.NewStaticCredentialsProvider(
			cfg.AWSAccessKeyID,
			cfg.AWSSecretAccessKey,
			"")))
	cfgOptions = append(cfgOptions, config.WithHTTPClient(httpClient))

	awsCfg, err := config.LoadDefaultConfig(ctx, cfgOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to load aws config with given credentials : %w", err)
	}
	sdk.Logger(ctx).Info().Msg("loaded destination aws configuration")

	var kinesisOptions []func(*kinesis.Options)

	if cfg.AWSURL != "" {
		resolver, err := newEndpointResolver(cfg.AWSURL)
		if err != nil {
			return nil, fmt.Errorf("failed to create endpoint resolver: %w", err)
		}

		kinesisOptions = append(kinesisOptions, kinesis.WithEndpointResolverV2(resolver))
	}

	return kinesis.NewFromConfig(awsCfg, kinesisOptions...), nil
}
