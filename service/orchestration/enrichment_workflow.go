package orchestration

import (
	"log"

	dao "github.com/bezalel-media-core/v2/dal"
	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
)

// Add child events based on script-output of Parent Media Event.
type EnrichmentWorkflow struct{}

func (s *EnrichmentWorkflow) GetWorkflowName() string {
	return "EnrichmentWorkflow"
}

const mediatTypeImage = "Image"

func (s *EnrichmentWorkflow) Run(ledgerItem tables.Ledger) error {
	// TODO: Support images for parent event.
	mediaEvents, err := dao.GetExistingMediaEvents(ledgerItem)
	if err != nil {
		log.Printf("correlationID: %s error extracting media events from ledger item: %s", ledgerItem.LedgerID, err)
		return err
	}

	for _, parentMedia := range mediaEvents {
		if isParentMediaEvent(parentMedia) {
			err = spawnChildMediaEvents(ledgerItem, parentMedia, mediaEvents)
			if err != nil {
				log.Printf("correlationID: %s failed to spawn child media events: %s", ledgerItem.LedgerID, err)
				return err
			}
		}
	}

	return err
}

func isParentMediaEvent(mediaEvent tables.MediaEvent) bool {
	return mediaEvent.ParentEventID == ""
}

func spawnChildMediaEvents(ledgerItem tables.Ledger, parentMediaEvent tables.MediaEvent, existingMediaEvents []tables.MediaEvent) error {
	return nil
}
