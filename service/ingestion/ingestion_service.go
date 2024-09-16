package ingestion

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/google/uuid"

	dal "github.com/bezalel-media-core/v2/dal"
	dynamo_tables "github.com/bezalel-media-core/v2/dal/tables/v1"
	source_events "github.com/bezalel-media-core/v2/service/ingestion/models/v1"
)

func HandleSourceEvent(source string, r *http.Request) error {
	decoder := json.NewDecoder(r.Body)
	var payload source_events.Raw_Event
	err := decoder.Decode(&payload)
	if err != nil {
		return err
	}
	payload.Source = source
	log.Println(payload)
	test()
	return err
}

func test() {
	ledgerId := uuid.New().String()
	entry := dynamo_tables.Ledger{
		LedgerID: ledgerId,
	}
	err := dal.CreateLedger(entry)
	if err != nil {
		log.Fatalf("failed to create ledger")
	}
	_, err = dal.GetLedger(ledgerId)
	if err != nil {
		log.Fatalf("failed to get ledger")
	}
	scriptEvent := dynamo_tables.ScriptEvent{
		Foo: "Hello world",
	}
	scriptEvents := []dynamo_tables.ScriptEvent{scriptEvent}

	err = dal.AppendLedgerScriptEvents(ledgerId, scriptEvents)
	if err != nil {
		log.Fatalf("failed to append script event to ledger")
	}
}
