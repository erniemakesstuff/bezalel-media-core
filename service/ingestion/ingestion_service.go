package ingestion

import (
	"log"
	"net/http"

	env "github.com/bezalel-media-core/v2/configuration"
	dal "github.com/bezalel-media-core/v2/dal"
)

func SaveSourceEventToLedger(source string, r *http.Request) error {
	driver, err := GetDriver(source, r.Body)
	if err != nil {
		log.Printf("error retreiving driver: %s", err)
	}

	if !driver.IsReady() {
		return nil
	}

	ledgerItem, err := driver.BuildEventPayload()
	if err != nil {
		log.Printf("driver failed to get raw event payload: %s", err)
		return err
	}

	entry, err := dal.GetHashEntry(ledgerItem.TriggerEventContentHash)
	if err != nil {
		log.Printf("failed to get hash entry: %s", err)
	}

	if len(entry.EventHash) != 0 {
		log.Printf("duplicate ingestion event, skipping: %s", ledgerItem.TriggerEventContentHash)
		return nil
	}

	if dal.IsOverflow(ledgerItem.TriggerEventSource, env.GetEnvConfigs().MaxSourceOverflow) {
		log.Printf("source has surplus unprocessed events - load shedding new events: %s", ledgerItem.TriggerEventSource)
		return nil
	}

	// Triggers downstream workflows via CDC on dynamo table.
	err = dal.CreateLedger(ledgerItem)
	if err != nil {
		log.Printf("failed to create a new ledger item: %s", err)
		return err
	}

	err = dal.CreateHashEntry(ledgerItem.TriggerEventContentHash)
	if err != nil {
		log.Printf("failed to create a new hash entry: %s", err)
		return err
	}

	return err
}

func AggregateSources(source string, r *http.Request) error {
	return nil
}
