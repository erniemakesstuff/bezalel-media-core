package orchestration

import (
	"fmt"
	"log"
	"time"

	"github.com/bezalel-media-core/v2/dal"
	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
	"github.com/bezalel-media-core/v2/manifest"
)

// AKA reaper workflow
type CompletionWorkflow struct{}

func (s *CompletionWorkflow) GetWorkflowName() string {
	return "CompletionWorkflow"
}

func (s *CompletionWorkflow) Run(ledgerItem tables.Ledger, processId string) error {
	isSyndicated, err := s.isFullySyndicated(ledgerItem)
	if err != nil {
		log.Printf("correlationID: %s error determining syndication status: %s", ledgerItem.LedgerID, err)
		return err
	}
	if !isSyndicated {
		log.Printf("correlationID: %s ledger is not fully syndicated; cannot complete", ledgerItem.LedgerID)
		return nil
	}

	err = s.expireLocks(ledgerItem)
	if err != nil {
		log.Fatalf("correlationID: %s error with expireLocks in completion workflow: %s", ledgerItem.LedgerID, err)
		return err
	}

	err = dal.SetLedgerStatus(ledgerItem, tables.FINISHED_LEDGER)
	if err != nil {
		log.Fatalf("correlationID: %s unable to mark ledger as completed: %s", ledgerItem.LedgerID, err)
		return err
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

	if len(publishEvents) == 0 || len(mediaEvents) == 0 {
		// If no events, not syndicated anywhere.
		return false, nil
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
			log.Printf("correlationID: %s missing syndication to %s of %s for eventId: %s", rootMedia.LedgerID, cn, channelNames, rootMedia.GetEventID())
			return false
		}
	}
	return true
}

func (s CompletionWorkflow) expireLocks(ledgerItem tables.Ledger) error {
	pubEvents, err := ledgerItem.GetExistingPublishEvents()
	if err != nil {
		log.Printf("correlationID: %s error retrieving publish events in expireLocks: %s", ledgerItem.LedgerID, err)
	}
	pubIdToPubs := PubStateByPubEventID(pubEvents)
	for _, p := range pubEvents {
		if s.isUnmarkedExpired(p, pubIdToPubs) {
			err = s.setExpiredPubEvent(p)
			if err != nil {
				return err
			}
			err = s.releaseAnyExpiredPublisherProfileLocks(p)
			if err != nil {
				return err
			}
		}
	}
	return err
}

func (s CompletionWorkflow) isUnmarkedExpired(pubEvent tables.PublishEvent, pubStateMap map[string]tables.PublishEvent) bool {
	keyAssigned := fmt.Sprintf("%s.%s.%s", pubEvent.DistributionChannel, pubEvent.PublisherProfileID, tables.ASSIGNED)
	keyTerminalCom := fmt.Sprintf("%s.%s.%s", pubEvent.DistributionChannel, pubEvent.PublisherProfileID, tables.COMPLETE)
	keyTerminalExp := fmt.Sprintf("%s.%s.%s", pubEvent.DistributionChannel, pubEvent.PublisherProfileID, tables.EXPIRED)
	_, isAssigned := pubStateMap[keyAssigned]
	_, isComplete := pubStateMap[keyTerminalCom]
	_, isExpired := pubStateMap[keyTerminalExp]
	if !isAssigned {
		return false
	}
	isAlreadyMarkedTerminal := isComplete || isExpired
	if isAlreadyMarkedTerminal {
		return false
	}

	timeNow := time.Now().UnixMilli()
	return pubEvent.ExpiresAtTTL < timeNow
}

func (s CompletionWorkflow) setExpiredPubEvent(pubEvent tables.PublishEvent) error {
	expiredEvent := pubEvent
	expiredEvent.PublishStatus = tables.EXPIRED
	err := dal.AppendLedgerPublishEvents(pubEvent.LedgerID, []tables.PublishEvent{expiredEvent})
	if err != nil {
		log.Printf("correlationID: %s error appending expired event in setExpired: %s", pubEvent.LedgerID, err)
		return err
	}
	return err
}

func (s CompletionWorkflow) releaseAnyExpiredPublisherProfileLocks(pubEvent tables.PublishEvent) error {
	profile, err := dal.GetPublisherAccount(pubEvent.OwnerAccountID, pubEvent.PublisherProfileID)
	if err != nil {
		log.Printf("correlationID: %s error loading publisher profile: %s", pubEvent.LedgerID, err)
		return err
	}
	timeNow := time.Now().UnixMilli()
	if profile.AssignmentLockTTL > timeNow || profile.PublishLockTTL > timeNow {
		log.Printf("correlationID: %s valid TTLs on publisherProfile; keeping locks: acc: %s pub: %s",
			pubEvent.LedgerID, pubEvent.OwnerAccountID, pubEvent.PublisherProfileID)
		return nil
	}

	err = dal.ForceAllLocksFree(pubEvent.OwnerAccountID, pubEvent.PublisherProfileID)
	if err != nil {
		log.Printf("correlationID: %s error release publishProfile locks in releasePublishProfile: %s", pubEvent.LedgerID, err)
		return err
	}

	return err
}
