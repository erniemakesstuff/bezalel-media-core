package drivers

import (
	"encoding/json"
	"fmt"
	"io"
	"log"

	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
	models_v1 "github.com/bezalel-media-core/v2/service/ingestion/models/v1"
)

type ForumDriver struct {
	PayloadIO io.ReadCloser
	Source    string
}

func NewForumDriver(payloadIO io.ReadCloser, source string) Driver {
	return &ForumDriver{PayloadIO: payloadIO, Source: source}
}

func (d ForumDriver) WithMedia(payloadIO io.ReadCloser) error {
	return nil
}

func (d ForumDriver) IsReady() bool {
	return true
}

func (d ForumDriver) BuildEventPayload() (tables.Ledger, error) {
	rawEvent, err := d.decode(d.PayloadIO)
	if err != nil {
		log.Printf("error decoding raw event payload: %s", err)
	}
	payload := fmt.Sprintf(`
		Main Post:
		%s

		Post Comments:
		%s
		`, rawEvent.ForumMainPost, rawEvent.Comments)
	return newLedgerFromText(rawEvent.TargetLanguage, payload, d.Source), err
}

func (d ForumDriver) decode(payloadIO io.ReadCloser) (models_v1.ForumDumpRequest, error) {
	decoder := json.NewDecoder(payloadIO)
	var payload models_v1.ForumDumpRequest
	err := decoder.Decode(&payload)
	if err != nil {
		return payload, err
	}
	return payload, err
}
