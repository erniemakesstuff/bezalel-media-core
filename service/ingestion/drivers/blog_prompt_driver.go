package drivers

import (
	"encoding/json"
	"io"
	"log"

	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
	models_v1 "github.com/bezalel-media-core/v2/service/ingestion/models/v1"
)

type BlogPromptDriver struct {
	PayloadIO io.ReadCloser
	Source    string
}

func NewBlogPromptDriver(payloadIO io.ReadCloser, source string) Driver {
	return &BlogPromptDriver{PayloadIO: payloadIO, Source: source}
}

func (d BlogPromptDriver) IsReady() bool {
	return true
}

func (d BlogPromptDriver) WithMedia(payloadIO io.ReadCloser) error {
	return nil
}

func (d BlogPromptDriver) BuildEventPayload() (tables.Ledger, error) {
	rawEvent, err := d.decode(d.PayloadIO)
	if err != nil {
		log.Printf("error decoding raw event payload: %s", err)
	}

	return newLedgerFromText(rawEvent.TargetLanguage, rawEvent.Text, d.Source), err
}

func (d BlogPromptDriver) decode(payloadIO io.ReadCloser) (models_v1.BlogRequest, error) {
	decoder := json.NewDecoder(payloadIO)
	var payload models_v1.BlogRequest
	err := decoder.Decode(&payload)
	if err != nil {
		return payload, err
	}
	return payload, err
}
