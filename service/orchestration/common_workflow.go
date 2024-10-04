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
	// TODO: Check if media exists in PgVector for-reuse.
	err = publishMediaGenerationSNS(mediaEvent)
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

func publishMediaGenerationSNS(mediaEvent tables.MediaEvent) error {
	return PublishMediaTopicSns(mediaEvent)
}

func ExistsInLedger(ledgerItem tables.Ledger, mediaEvent tables.MediaEvent) (bool, error) {
	existingMediaEvents, err := ledgerItem.GetExistingMediaEvents()
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

func IsParentMediaEvent(mediaEvent tables.MediaEvent) bool {
	return mediaEvent.ParentEventID == ""
}

func AllChildrenRendered(mediaEventRoot tables.MediaEvent, mediaEvents []tables.MediaEvent) bool {
	exists, err := MediaExists(mediaEventRoot.ContentLookupKey)
	if err != nil {
		log.Printf("unexpected mediaExists error: %s", err)
		return false
	}
	if !exists {
		log.Printf("root media not set: %s", mediaEventRoot.ContentLookupKey)
		return false
	}

	for _, m := range mediaEvents {
		if len(m.ParentEventID) == 0 || m.ParentEventID != mediaEventRoot.GetEventID() {
			continue
		}

		exists, err = MediaExists(m.ContentLookupKey)
		if err != nil {
			log.Printf("unexpected mediaExists error: %s", err)
			return false
		}
		if !exists {
			return false
		}
	}
	return true
}
