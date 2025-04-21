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

type Config struct {
	// The amazon access key id
	AWSAccessKeyID string `json:"aws.accessKeyId" validate:"required"`
	// The amazon secret access key
	AWSSecretAccessKey string `json:"aws.secretAccessKey" validate:"required"`
	// The region where the stream is hosted
	AWSRegion string `json:"aws.region" validate:"required"`
	// The URL for endpoint override - testing/dry-run only
	AWSURL string `json:"aws.url"`
}
