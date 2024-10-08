package orchestration

import (
	"log"

	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
)

// Add child events based on script-output of Parent Media Event.
type EnrichmentWorkflow struct{}

func (s *EnrichmentWorkflow) GetWorkflowName() string {
	return "EnrichmentWorkflow"
}

const mediatTypeImage = "Image"

func (s *EnrichmentWorkflow) Run(ledgerItem tables.Ledger, processId string) error {
	// TODO: Support images for parent event.
	// TODO: Set position layer, and render sequence
	//		Audio and Video can have the same RenderSequence if they are concurrent, or the media template allows (e.g. splitscreen)
	// TODO: Localize language content by spawning new-root events for destination languages.
	// WAIT for 30 minutes; periodically polling contentLookupKey to see if finished.
	//	Set status EXPIRED on timeout.
	mediaEvents, err := ledgerItem.GetExistingMediaEvents()
	if err != nil {
		log.Printf("correlationID: %s error extracting media events from ledger item: %s", ledgerItem.LedgerID, err)
		return err
	}

	for _, parentMedia := range mediaEvents {
		if IsParentMediaEvent(parentMedia) {
			err = spawnChildMediaEvents(ledgerItem, parentMedia, mediaEvents)
			if err != nil {
				log.Printf("correlationID: %s failed to spawn child media events: %s", ledgerItem.LedgerID, err)
				return err
			}
		}
	}

	return err
}

func spawnChildMediaEvents(ledgerItem tables.Ledger, parentMediaEvent tables.MediaEvent, existingMediaEvents []tables.MediaEvent) error {
	// TODO:
	// Parse script json
	// Output children media events w/ incrementing RenderSequence attribute set.
	// Set PositionLayer instruction.
	// Call HandleMediaGeneration
	return nil
}
