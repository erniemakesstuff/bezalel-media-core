package ingestion

import (
	"errors"
	"io"

	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
	"github.com/bezalel-media-core/v2/service/ingestion/drivers"
)

type Driver interface {
	GetRawEventPayload() (tables.Ledger, error)
}

func GetDriver(source string, payloadIO io.ReadCloser) (Driver, error) {
	switch {
	case source == "v1/source/prompt":
		val := drivers.CustomPromptDriver{PayloadIO: payloadIO, Source: source}
		return val, nil
	case source == "v1/source/blog":
		val := drivers.BlogPromptDriver{PayloadIO: payloadIO, Source: source}
		return val, nil
	}
	return nil, errors.New("no matching source-to-driver found")
}
