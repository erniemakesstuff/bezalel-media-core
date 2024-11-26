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

func (d CustomPromptDriver) GetRawEventPayload() (tables.Ledger, error) {
	rawEvent, err := d.decode(d.PayloadIO)
	if err != nil {
		log.Printf("error decoding raw event payload: %s", err)
	}

	return newLedgerFromText("EN", rawEvent.PromptText, d.Source), err
}

func (d CustomPromptDriver) decode(payloadIO io.ReadCloser) (models_v1.Custom_Prompt_Request, error) {
	decoder := json.NewDecoder(payloadIO)
	var payload models_v1.Custom_Prompt_Request
	err := decoder.Decode(&payload)
	if err != nil {
		return payload, err
	}
	return payload, err
}
