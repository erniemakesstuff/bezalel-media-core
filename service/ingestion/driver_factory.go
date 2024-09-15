package ingestion

import (
	dynamo_tables "github.com/bezalel-media-core/v2/dal/tables/v1"
)

type Driver interface {
	GetRawEventPayload() (dynamo_tables.Ledger, error)
}

func GetDriver(source string, payloadJson string) (Driver, error) {
	// TODO: Business logic to select appropriate driver given the source.
	return nil, nil
}
