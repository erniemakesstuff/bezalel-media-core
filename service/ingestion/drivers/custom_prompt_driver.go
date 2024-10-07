package drivers

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"

	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
	models_v1 "github.com/bezalel-media-core/v2/service/ingestion/models/v1"
	"github.com/google/uuid"
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

	ledger := tables.Ledger{
		LedgerID:                uuid.New().String(),
		LedgerStatus:            tables.NEW_LEDGER,
		TriggerEventPayload:     rawEvent.PromptText,
		TriggerEventSource:      d.Source,
		TriggerEventContentHash: d.getMD5Hash(rawEvent.PromptText), // Set, but prompts aren't deduped.
		TriggerEventLanguage:    "EN",
	}
	return ledger, err
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

func (d CustomPromptDriver) getMD5Hash(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}
