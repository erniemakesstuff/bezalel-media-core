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
		log.Printf("correlationID: %s media event already in ledger", ledgerItem.LedgerID)
		return nil
	}
	// TODO: Check if media exists in PgVector for-reuse.
	err = publishMediaGenerationSNS(mediaEvents)
	if err != nil {
		return err
	}
	err = dao.AppendLedgerMediaEvents(ledgerItem.LedgerID, mediaEvents)
	return err
}

func publishMediaGenerationSNS(mediaEvents []tables.MediaEvent) error {
	var err error
	for _, m := range mediaEvents {
		err = PublishMediaTopicSns(m)
	}
	return err
}

func ExistsInLedger(ledgerItem tables.Ledger, mediaEvents []tables.MediaEvent) (bool, error) {
	existingMediaEvents, err := ledgerItem.GetExistingMediaEvents()
	if err != nil {
		log.Printf("correlationID: %s error deserializing existing media events from ledger: %s", ledgerItem.LedgerID, err)
		return false, err
	}
	existingMediaEventsMap := make(map[string]bool)
	for _, m := range existingMediaEvents {
		existingMediaEventsMap[m.GetEventID()] = true
	}
	for _, m := range mediaEvents {
		if _, ok := existingMediaEventsMap[m.GetEventID()]; !ok {
			log.Printf("media event found in ledger, cannot duplicate: %s", m.GetEventID())
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
		if len(m.ParentEventID) == 0 || m.ParentEventID != rootId || m.GetEventID() != rootId {
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

func CollectChildrenEvents(mediaEventRoot tables.MediaEvent, mediaEvents []tables.MediaEvent) []tables.MediaEvent {
	result := []tables.MediaEvent{}
	for _, m := range mediaEvents {
		if len(m.ParentEventID) == 0 || m.ParentEventID != mediaEventRoot.GetEventID() {
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
		p, ok := publisherIdMap[m.GetEventID()]
		if !ok {
			continue
		}
		result[m.GetEventID()] = append(result[m.GetEventID()], p...)
	}
	return result
}
