package ingestion

import (
	"log"
	"net/http"

	dal "github.com/bezalel-media-core/v2/dal"
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

	entry, err := dal.GetHashEntry(ledgerItem.TriggerEventContentHash)
	if err != nil {
		log.Printf("failed to get hash entry: %s", err)
	}

	if len(entry.EventHash) != 0 {
		log.Printf("duplicate ingestion event, skipping: %s", ledgerItem.TriggerEventContentHash)
		return nil
	}

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
