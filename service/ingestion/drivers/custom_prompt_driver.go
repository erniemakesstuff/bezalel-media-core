package drivers

import (
	"encoding/json"
	"io"
	"log"

	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
	models_v1 "github.com/bezalel-media-core/v2/service/ingestion/models/v1"
)

type CustomPromptDriver struct {
	PayloadIO io.ReadCloser
	Source    string
}

func NewCustomPromptDriver(payloadIO io.ReadCloser, source string) Driver {
	return &CustomPromptDriver{PayloadIO: payloadIO, Source: source}
}

func (d CustomPromptDriver) WithMedia(payloadIO io.ReadCloser) error {
	return nil
}

func (d CustomPromptDriver) IsReady() bool {
	return true
}

func (d CustomPromptDriver) BuildEventPayload() (tables.Ledger, error) {
	rawEvent, err := d.decode(d.PayloadIO)
	if err != nil {
		log.Printf("error decoding raw event payload: %s", err)
	}

	return newLedgerFromText("EN", rawEvent.PromptText, d.Source), err
}

func (d CustomPromptDriver) decode(payloadIO io.ReadCloser) (models_v1.CustomPromptRequest, error) {
	decoder := json.NewDecoder(payloadIO)
	var payload models_v1.CustomPromptRequest
	err := decoder.Decode(&payload)
	if err != nil {
		return payload, err
	}
	return payload, err
}
