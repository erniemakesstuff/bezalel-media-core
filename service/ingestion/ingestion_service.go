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
	// TODO: Dedupe RawContentHash prior to creating a new ledger item.

	err = dal.CreateLedger(ledgerItem)
	if err != nil {
		log.Printf("failed to create a new ledger item: %s", err)
	}
	return err
}
