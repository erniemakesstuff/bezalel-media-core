package orchestration

import (
	"log"
	"time"

	"github.com/bezalel-media-core/v2/dal"
	dao "github.com/bezalel-media-core/v2/dal"
	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
)

func HandleMediaGeneration(ledgerItem tables.Ledger, mediaEvents []tables.MediaEvent) error {
	existsInLedger, err := ExistsInLedger(ledgerItem, mediaEvents)
	if err != nil {
		log.Printf("correlationID: %s unable to determine idempotency: %s", ledgerItem.LedgerID, err)
		return err
	}
	if existsInLedger {
		return nil
	}

	// TODO: Check if media exists in PgVector for-reuse.
	// When exists in Pgvector; write-replace the contentLookup key!
	// Mock this for MVP for static background and audio.
	// If media does not exist; invalidate pgvector entry.

	// TODO: ignore pgvector for metadata entries; call m.IsMetadata...
	err = publishMediaGenerationSNS(mediaEvents)
	if err != nil {
		return err
	}
	err = dao.AppendLedgerMediaEvents(ledgerItem.LedgerID, mediaEvents)
	return err
}

func publishMediaGenerationSNS(mediaEvents []tables.MediaEvent) error {
	for _, m := range mediaEvents {
		if m.IsMetaPurposeOnly() {
			continue
		}
		alreadyGenerated, err := MediaExists(m.ContentLookupKey)
		if err != nil {
			return err
		}

		if alreadyGenerated {
			continue
		}

		err = PublishMediaTopicSns(m)
		if err != nil {
			return err
		}
	}
	return nil
}

func ExistsInLedger(ledgerItem tables.Ledger, mediaEvents []tables.MediaEvent) (bool, error) {
	existingMediaEvents, err := ledgerItem.GetExistingMediaEvents()
	if err != nil {
		log.Printf("correlationID: %s error deserializing existing media events from ledger: %s", ledgerItem.LedgerID, err)
		return false, err
	}
	existingMediaEventsMap := make(map[string]bool)
	for _, m := range existingMediaEvents {
		existingMediaEventsMap[m.EventID] = true
	}
	for _, m := range mediaEvents {
		if _, ok := existingMediaEventsMap[m.EventID]; !ok {
			return false, nil
		}
	}
	return true, nil
}

func IsParentMediaEvent(mediaEvent tables.MediaEvent) bool {
	return mediaEvent.ParentEventID == ""
}

// Call when appending a soft-lock event to ledger such as ASSIGN or PUBLISHING
// to verify that you own the ASSIGN or PUBLISH
func WaitOptimisticVerifyWroteLedger(expectedPublisherEventID string, ledgerId string) (bool, error) {
	time.Sleep(time.Duration(5) * time.Second)

	ledgerItem, err := dal.GetLedger(ledgerId)
	if err != nil {
		log.Printf("correlationID: %s failed to fetch event ledger for verification: %s", ledgerId, err)
		return false, err
	}

	existingPublishEvents, err := ledgerItem.GetExistingPublishEvents()
	if err != nil {
		log.Printf("correlationID: %s error retreiving existing publish events for verification: %s", ledgerId, err)
		return false, err
	}

	for _, p := range existingPublishEvents {
		if p.GetEventID() == expectedPublisherEventID {
			return true, nil
		}
	}
	return false, err
}

func AllChildrenRendered(rootId string, mediaEvents []tables.MediaEvent) bool {
	for _, m := range mediaEvents {
		if len(m.ParentEventID) == 0 || m.ParentEventID != rootId ||
			m.EventID == rootId || m.IsMetaPurposeOnly() {
			continue
		}

		exists, err := MediaExists(m.ContentLookupKey)
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

func CollectNonMetaChildMedia(mediaEventRootId string, mediaEvents []tables.MediaEvent) []tables.MediaEvent {
	result := []tables.MediaEvent{}
	for _, m := range mediaEvents {
		if len(m.ParentEventID) == 0 || m.ParentEventID != mediaEventRootId || m.IsMetaPurposeOnly() {
			continue
		}
		result = append(result, m)
	}
	return result
}

func CollectChildMediaEligibleForFinalRender(mediaEventRootId string, mediaEvents []tables.MediaEvent) []tables.MediaEvent {
	result := []tables.MediaEvent{}
	for _, m := range CollectNonMetaChildMedia(mediaEventRootId, mediaEvents) {
		if m.MetaMediaDescriptor == tables.FINAL_RENDER {
			// Existing final render events are inelligible for another final render.
			continue
		}

		result = append(result, m)
	}
	return result
}

func PubStateByRootMedia(publishEvents []tables.PublishEvent) map[string]tables.PublishEvent {
	result := make(map[string]tables.PublishEvent)
	if len(publishEvents) == 0 {
		return result
	}
	for _, p := range publishEvents {
		result[p.GetRootMediaAssignmentKey()] = p
	}
	return result
}

func PubStateByPubEventID(publishEvents []tables.PublishEvent) map[string]tables.PublishEvent {
	result := make(map[string]tables.PublishEvent)
	if len(publishEvents) == 0 {
		return result
	}
	for _, p := range publishEvents {
		result[p.GetEventID()] = p
	}
	return result
}

func CreateMediaEventToPublisherMap(publishEvents []tables.PublishEvent, mediaEvents []tables.MediaEvent) map[string][]tables.PublishEvent {
	result := make(map[string][]tables.PublishEvent)
	if len(publishEvents) == 0 || len(mediaEvents) == 0 {
		log.Printf("WARN returning empty map")
		return result
	}

	publisherIdMap := make(map[string][]tables.PublishEvent)
	for _, p := range publishEvents {
		publisherIdMap[p.RootMediaEventID] = append(publisherIdMap[p.RootMediaEventID], p)
	}

	for _, m := range mediaEvents {
		if m.IsMetaPurposeOnly() {
			continue
		}

		p, ok := publisherIdMap[m.EventID]
		if !ok {
			continue
		}
		result[m.EventID] = append(result[m.EventID], p...)
	}
	return result
}

func CreateMediaMapByEventId(mediaEvents []tables.MediaEvent) map[string]tables.MediaEvent {
	result := make(map[string]tables.MediaEvent)
	if len(mediaEvents) == 0 {
		return result
	}
	for _, m := range mediaEvents {
		result[m.EventID] = m
	}
	return result
}
