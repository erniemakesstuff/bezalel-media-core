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

func (d ForumDriver) GetRawEventPayload() (tables.Ledger, error) {
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

func (d ForumDriver) decode(payloadIO io.ReadCloser) (models_v1.Forum_Dump_Request, error) {
	decoder := json.NewDecoder(payloadIO)
	var payload models_v1.Forum_Dump_Request
	err := decoder.Decode(&payload)
	if err != nil {
		return payload, err
	}
	return payload, err
}
