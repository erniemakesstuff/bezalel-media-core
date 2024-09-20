package ingestion

import (
	"log"
	"net/http"

	"github.com/google/uuid"

	dal "github.com/bezalel-media-core/v2/dal"
	dynamo_tables "github.com/bezalel-media-core/v2/dal/tables/v1"
)

func SaveSourceEventToLedger(source string, r *http.Request) error {
	driver, err := GetDriver(source, r.Body)
	ledgerItem, err := driver.GetRawEventPayload()
	if err != nil {
		log.Printf("driver failed to get raw event payload: %s", err)
		return err
	}

	err = dal.CreateLedger(ledgerItem)
	if err != nil {
		log.Printf("failed to create a new ledger item: %s", err)
	}
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
	scriptEvent1 := dynamo_tables.ScriptEvent{
		ContentLookupKey: "Hello world",
		Language:         "EN",
		ContentType:      "Video",
		Niche:            "NewsBroadcast",
	}
	scriptEvent2 := dynamo_tables.ScriptEvent{
		ContentLookupKey: "Hello world",
		Language:         "EN",
		ContentType:      "Image",
		Niche:            "Reaction",
	}
	scriptEvents := []dynamo_tables.ScriptEvent{scriptEvent1, scriptEvent2}

	err = dal.AppendLedgerScriptEvents(ledgerId, scriptEvents)
	if err != nil {
		log.Fatalf("failed to append script event to ledger")
	}
}
