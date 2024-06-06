// Code generated by paramgen. DO NOT EDIT.
// Source: github.com/ConduitIO/conduit-connector-sdk/tree/main/cmd/paramgen

package source

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func (Config) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		"aws.accessKeyId": {
			Default:     "",
			Description: "aws.accessKeyId is the amazon access key id",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{
				sdk.ValidationRequired{},
			},
		},
		"aws.region": {
			Default:     "",
			Description: "aws.region is the region where the stream is hosted",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{
				sdk.ValidationRequired{},
			},
		},
		"aws.secretAccessKey": {
			Default:     "",
			Description: "aws.secretAccessKey is the amazon secret access key",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{
				sdk.ValidationRequired{},
			},
		},
		"aws.url": {
			Default:     "",
			Description: "aws.url is the URL for endpoint override - testing/dry-run only",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{},
		},
		"startFromLatest": {
			Default:     "false",
			Description: "startFromLatest defaults to false. When true, sets the iterator type to LATEST (iterates from the point that the connection begins = CDC). when false, sets the iterator type to TRIM_HORIZON (iterates from the oldest record in the shard = snapshot). Iterators eventually shift to latest after snapshot has been written",
			Type:        sdk.ParameterTypeBool,
			Validations: []sdk.Validation{},
		},
		"streamARN": {
			Default:     "",
			Description: "streamARN is the Kinesis stream's Amazon Resource Name",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{
				sdk.ValidationRequired{},
			},
		},
		"streamName": {
			Default:     "",
			Description: "streamName is the name of the Kinesis Data Stream",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{},
		},
	}
}
