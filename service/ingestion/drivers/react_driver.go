package drivers

import (
	"encoding/json"
	"io"
	"log"

	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
	models_v1 "github.com/bezalel-media-core/v2/service/ingestion/models/v1"
)

type ReactDriver struct {
	PayloadIO io.ReadCloser
	Source    string
}

// TODO: create multiple ReactDriver instances
// Maintain six ReactDriver instances: short videos, long videos, short images, long images, short audio, long audio
func NewReactDriver(payloadIO io.ReadCloser, source string) Driver {
	return &ReactDriver{PayloadIO: payloadIO, Source: source}
}

func (d ReactDriver) IsReady() bool {
	return true
}

func (d ReactDriver) GetRawEventPayload() (tables.Ledger, error) {
	// TODO: This is not threadsafe.
	rawEvent, err := d.decode(d.PayloadIO)
	if err != nil {
		log.Printf("error decoding raw event payload: %s", err)
	}

	return newLedgerFromText(rawEvent.TargetLanguage, rawEvent.Text, d.Source), err
}

func (d ReactDriver) decode(payloadIO io.ReadCloser) (models_v1.Blog_Request, error) {
	decoder := json.NewDecoder(payloadIO)
	var payload models_v1.Blog_Request
	err := decoder.Decode(&payload)
	if err != nil {
		return payload, err
	}
	return payload, err
}
