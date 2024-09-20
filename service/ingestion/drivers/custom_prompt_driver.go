package drivers

import (
	"encoding/json"
	"hash/fnv"
	"io"
	"log"
	"strconv"
	"time"

	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
	models_v1 "github.com/bezalel-media-core/v2/service/ingestion/models"
	"github.com/google/uuid"
)

type CustomPromptDriver struct {
	PayloadIO io.ReadCloser
	Source    string
}

func (d CustomPromptDriver) GetRawEventPayload() (tables.Ledger, error) {
	rawEvent, err := decode(d.PayloadIO)
	if err != nil {
		log.Printf("error decoding raw event payload: %s", err)
	}

	ledger := tables.Ledger{
		LedgerID:                  uuid.New().String(),
		LedgerStatus:              tables.NEW_LEDGER,
		LedgerCreatedAtEpochMilli: time.Now().UnixMilli(),
		RawEventPayload:           rawEvent.Prompt,
		RawEventSource:            d.Source,
	}
	return ledger, err
}

func decode(payloadIO io.ReadCloser) (models_v1.Raw_Event, error) {
	decoder := json.NewDecoder(payloadIO)
	var payload models_v1.Raw_Event
	err := decoder.Decode(&payload)
	if err != nil {
		return payload, err
	}
	return payload, err
}

func hash(s string) string {
	h := fnv.New32a()
	h.Write([]byte(s))
	return strconv.FormatUint(uint64(h.Sum32()), 10)
}
