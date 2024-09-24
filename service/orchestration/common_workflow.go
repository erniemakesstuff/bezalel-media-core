package orchestration

import (
	"log"

	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
)

func HandleMediaGeneration(ledgerItem tables.Ledger, mediaEvent tables.MediaEvent) error {
	existsInLedger, err := existsInLedger(ledgerItem, mediaEvent)
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
	// TODO: appendMediaEvents
	return nil
}

func publishToSNSToGenerateMedia(mediaEvent tables.MediaEvent) error {

	return PublishMediaTopicSns(mediaEvent)
}

func appendMediaEvents(ledgerItem tables.Ledger, mediaEvent tables.MediaEvent) ([]tables.MediaEvent, error) {
	return []tables.MediaEvent{}, nil
}

func existsInLedger(ledgerItem tables.Ledger, mediaEvent tables.MediaEvent) (bool, error) {
	// TODO:
	return false, nil
}
