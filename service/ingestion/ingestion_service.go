package ingestion

import (
	"log"
	"net/http"

	"github.com/google/uuid"

	dal "github.com/bezalel-media-core/v2/dal"
	dynamo_tables "github.com/bezalel-media-core/v2/dal/tables/v1"
	service "github.com/bezalel-media-core/v2/service/orchestration"
)

func SaveSourceEventToLedger(source string, r *http.Request) error {
	driver, err := GetDriver(source, r.Body)
	if err != nil {
		log.Printf("error retreiving driver: %s", err)
	}
	ledgerItem, err := driver.GetRawEventPayload()
	if err != nil {
		log.Printf("driver failed to get raw event payload: %s", err)
		return err
	}
	err = dal.CreateLedger(ledgerItem)
	if err != nil {
		log.Printf("failed to create a new ledger item: %s", err)
	}
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
	scriptEvent1 := dynamo_tables.MediaEvent{
		ContentLookupKey: "Hello world",
		Language:         "EN",
		MediaType:        "Video",
		Niche:            "NewsBroadcast",
	}
	scriptEvent2 := dynamo_tables.MediaEvent{
		ContentLookupKey: "Hello world",
		Language:         "EN",
		MediaType:        "Image",
		Niche:            "Reaction",
	}
	scriptEvents := []dynamo_tables.MediaEvent{scriptEvent1, scriptEvent2}

	err = dal.AppendLedgerMediaEvents(ledgerId, scriptEvents)
	if err != nil {
		log.Fatalf("failed to append script event to ledger")
	}
	msg := dynamo_tables.MediaEvent{
		ContentLookupKey:        "FooBar",
		Niche:                   "Hello world",
		MediaType:               dynamo_tables.MEDIA_TEXT,
		DistributionFormat:      dynamo_tables.DIST_FORMAT_BLOG,
		SystemPromptInstruction: "You are a fun bubbly woman.",
		PromptInstruction:       "How do you like to spend sunny days?",
	}
	err = service.PublishMediaTopicSns(msg)
	if err != nil {
		log.Fatalf("failed to publish to media event sns: %s", err)
	}
}
