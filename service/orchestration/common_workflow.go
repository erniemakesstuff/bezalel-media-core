package orchestration

import (
	"log"

	dao "github.com/bezalel-media-core/v2/dal"
	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
)

func HandleMediaGeneration(ledgerItem tables.Ledger, mediaEvent tables.MediaEvent) error {
	existsInLedger, err := ExistsInLedger(ledgerItem, mediaEvent)
	if err != nil {
		log.Printf("correlationID: %s unable to determine idempotency: %s", ledgerItem.LedgerID, err)
		return err
	}
	if existsInLedger {
		log.Printf("correlationID: %s media event already in ledger eventID: %s", ledgerItem.LedgerID, mediaEvent.EventID)
		return nil
	}
	err = publishToSNSToGenerateMedia(mediaEvent)
	if err != nil {
		return err
	}
	err = appendMediaEventToLedgerItem(ledgerItem, mediaEvent)
	return err
}

func appendMediaEventToLedgerItem(ledgerItem tables.Ledger, mediaEvent tables.MediaEvent) error {
	mediaEvents := []tables.MediaEvent{mediaEvent}
	return dao.AppendLedgerMediaEvents(ledgerItem.LedgerID, mediaEvents)
}

func publishToSNSToGenerateMedia(mediaEvent tables.MediaEvent) error {
	return PublishMediaTopicSns(mediaEvent)
}

func ExistsInLedger(ledgerItem tables.Ledger, mediaEvent tables.MediaEvent) (bool, error) {
	existingMediaEvents, err := dao.GetExistingMediaEvents(ledgerItem)
	if err != nil {
		log.Printf("correlationID: %s error deserializing existing media events from ledger: %s", ledgerItem.LedgerID, err)
		return false, err
	}
	for _, m := range existingMediaEvents {
		if m.EventID == mediaEvent.GetEventID() {
			return true, nil
		}
	}
	return false, nil
}