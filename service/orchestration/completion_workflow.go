package orchestration

import (
	"fmt"
	"log"

	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
	"github.com/bezalel-media-core/v2/manifest"
)

// AKA reaper workflow
type CompletionWorkflow struct{}

func (s *CompletionWorkflow) GetWorkflowName() string {
	return "CompletionWorkflow"
}

func (s *CompletionWorkflow) Run(ledgerItem tables.Ledger, processId string) error {
	// TODO:
	// Mark LedgerItem COMPLETE if fully syndicated (examine PublishEvents FINISHED per-distributino channel)
	// Set PublishEvents to Expired if no corresponding FINISHED within TTL.
	//	Examine AssignmentLocks, and PublishLocks; publish invalidation events as needed.
	// ReleaseAssignmentLock, ReleasePublishLock
	isSyndicated, err := s.isFullySyndicated(ledgerItem)
	if err != nil {
		log.Printf("correlationID: %s error determining syndication status: %s", ledgerItem.LedgerID, err)
		return err
	}
	if !isSyndicated {
		log.Printf("correlationID: %s ledger is not fully syndicated; cannot complete", ledgerItem.LedgerID)
		return nil
	}
	return nil
}

func (s CompletionWorkflow) isFullySyndicated(ledgerItem tables.Ledger) (bool, error) {
	mediaEvents, err := ledgerItem.GetExistingMediaEvents()
	if err != nil {
		log.Printf("correlationID: %s error getting media events for syndication check: %s", ledgerItem.LedgerID, err)
		return false, err
	}
	publishEvents, err := ledgerItem.GetExistingPublishEvents()
	if err != nil {
		log.Printf("correlationID: %s error getting publish events for syndication check: %s", ledgerItem.LedgerID, err)
		return false, err
	}

	pubStateByRootMediaMap := PubStateByRootMedia(publishEvents)
	for _, m := range mediaEvents {
		if len(m.ParentEventID) != 0 {
			continue
		}
		distributionChannels := manifest.GetManifestLoader().ChannelNamesFromFormat(string(m.DistributionFormat))
		if !s.isPublishedOnAllChannels(distributionChannels, pubStateByRootMediaMap, m) {
			return false, nil
		}
	}
	return true, nil
}

func (s CompletionWorkflow) isPublishedOnAllChannels(channelNames []string,
	pubStateMap map[string]tables.PublishEvent, rootMedia tables.MediaEvent) bool {
	for _, cn := range channelNames {
		key := fmt.Sprintf("%s.%s.%s", cn, rootMedia.GetEventID(), tables.COMPLETE)
		_, ok := pubStateMap[key]
		if !ok {
			log.Printf("correlationID: %s missing syndication to %s for eventId: %s", rootMedia.LedgerID, cn, rootMedia.GetEventID())
			return false
		}
	}
	return false
}
