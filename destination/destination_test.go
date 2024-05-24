package destination

import (
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func TestCreatePutRequestInput(t *testing.T) {
	is := is.New(t)

	testDriver := sdk.ConfigurableAcceptanceTestDriver{}

	record1 := testDriver.GenerateRecord(t, sdk.OperationCreate)
	record1.Key = sdk.RawData("key1")
	record2 := testDriver.GenerateRecord(t, sdk.OperationCreate)
	record2.Key = sdk.RawData("key2")
	record3 := testDriver.GenerateRecord(t, sdk.OperationCreate)
	record3.Key = sdk.RawData("key3")
	record4 := testDriver.GenerateRecord(t, sdk.OperationCreate)

	// make last record have a key greater than 256 characters to trigger the key length overflow path
	var record4Key string
	for i := 0; i < 257; i++ {
		record4Key += "x"
	}
	record4.Key = sdk.RawData(record4Key)

	records := []sdk.Record{record1, record2, record3, record4}

	{
		dest := Destination{config: Config{PartitionKey: "partitionKey"}}

		request := dest.createPutRequestInput(records)

		for i, req := range request.Records {
			is.Equal(*req.PartitionKey, "partitionKey")
			is.Equal(req.Data, records[i].Bytes())
		}
	}

	{
		dest := Destination{config: Config{}}

		request := dest.createPutRequestInput(records)

		for i, req := range request.Records {
			is.Equal(req.Data, records[i].Bytes())
			if l := len(*req.PartitionKey); l > 3 {
				is.Equal(l, 256) // partition key should be about 256 characters
			}
		}
	}
}
