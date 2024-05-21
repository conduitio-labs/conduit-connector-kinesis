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
