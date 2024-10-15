package orchestration

import (
	"fmt"
	"log"
	"time"

	dal "github.com/bezalel-media-core/v2/dal"
	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
	drivers "github.com/bezalel-media-core/v2/service/orchestration/publisher-drivers"
)

type PublishWorkFlow struct{}

func (s *PublishWorkFlow) GetWorkflowName() string {
	return "PublishWorkFlow"
}

func (s *PublishWorkFlow) Run(ledgerItem tables.Ledger, processId string) error {
	publishCommands, err := s.collectPublishCommands(ledgerItem)
	if err != nil {
		log.Printf("correlationID: %s error generating publish commands: %s", ledgerItem.LedgerID, err)
		return err
	}
	if len(publishCommands) == 0 {
		log.Printf("correlationID: %s no publish commands created", ledgerItem.LedgerID)
	}
	for _, p := range publishCommands {
		err = s.handlePublish(p, ledgerItem.LedgerID, processId)
	}
	return err
}

func (s *PublishWorkFlow) handlePublish(pubCommand drivers.PublishCommand, ledgerId string, processId string) error {
	driver, err := drivers.GetDriver(pubCommand.RootPublishEvent.DistributionChannel)
	if err != nil {
		log.Printf("correlationID: %s error fetching driver: %s", ledgerId, err)
		return err
	}

	err = dal.TakePublishLock(pubCommand.RootPublishEvent.OwnerAccountID, pubCommand.RootPublishEvent.PublisherProfileID, processId)
	if err != nil {
		log.Printf("correlationID: %s error taking publisher lock: %s", ledgerId, err)
		dao, _ := dal.GetPublisherAccount(pubCommand.RootPublishEvent.OwnerAccountID, pubCommand.RootPublishEvent.PublisherProfileID)
		log.Printf("correlationID: %s current publock owner: %s attempted lockid: %s", ledgerId, dao.PublishLockID, processId)
		return err
	}

	renderEvent := pubCommand.RootPublishEvent
	renderEvent.ProcessOwner = processId
	renderEvent.PublishStatus = tables.PUBLISHING
	err = dal.AppendLedgerPublishEvents(ledgerId, []tables.PublishEvent{renderEvent})
	if err != nil {
		log.Printf("correlationID: %s error appending publisher publishing-event to ledger: %s", ledgerId, err)
		// Try release publish lock
		dal.ReleasePublishLock(pubCommand.RootPublishEvent.OwnerAccountID, pubCommand.RootPublishEvent.PublisherProfileID, processId)
		return err
	}

	isSuccessfullyLocked, err := WaitOptimisticVerifyWroteLedger(renderEvent.GetEventID(), ledgerId)
	if err != nil || !isSuccessfullyLocked {
		log.Printf("correlationID: %s unable to verify publish-event ledger softlock: %s", ledgerId, err)
		// Try release publish lock
		dal.ReleasePublishLock(pubCommand.RootPublishEvent.OwnerAccountID, pubCommand.RootPublishEvent.PublisherProfileID, processId)
		return err
	}

	err = driver.Publish(pubCommand)
	if err != nil {
		log.Printf("correlationID: %s error publishing: %s", ledgerId, err)
		return err
	}

	completionEventRecord := pubCommand.RootPublishEvent
	completionEventRecord.PublishStatus = tables.COMPLETE
	err = dal.AppendLedgerPublishEvents(ledgerId, []tables.PublishEvent{completionEventRecord})
	if err != nil {
		log.Printf("correlationID: %s error appending completion publish event: %s", ledgerId, err)
		return err
	}

	err = dal.RecordPublishTime(pubCommand.RootPublishEvent.OwnerAccountID, pubCommand.RootPublishEvent.PublisherProfileID)
	if err != nil {
		log.Printf("correlationID: %s error recording last publish time: %s", ledgerId, err)
		return err
	}

	err = dal.ForceAllLocksFree(pubCommand.RootPublishEvent.OwnerAccountID, pubCommand.RootPublishEvent.PublisherProfileID)
	if err != nil {
		log.Printf("correlationID: %s error releasing all locks for successful publish: %s", ledgerId, err)
		return err
	}

	return err
}

func (s *PublishWorkFlow) collectPublishCommands(ledgerItem tables.Ledger) ([]drivers.PublishCommand, error) {
	publishEvents, err := ledgerItem.GetExistingPublishEvents()
	if err != nil {
		log.Printf("correlationID: %s error getting existing publish events: %s", ledgerItem.LedgerID, err)
		return []drivers.PublishCommand{}, err
	}
	mediaEvents, err := ledgerItem.GetExistingMediaEvents()
	if err != nil {
		log.Printf("correlationID: %s error getting existing media events: %s", ledgerItem.LedgerID, err)
		return []drivers.PublishCommand{}, err
	}

	publishStateToPubMap := PubStateByRootMedia(publishEvents)
	result := []drivers.PublishCommand{}
	for _, p := range publishEvents {
		if s.isRenderWithoutPublish(p, publishStateToPubMap) && AllChildrenRendered(p.RootMediaEventID, mediaEvents) {
			finalRenderRoot := s.getFinalRenderRoot(p.RootMediaEventID, p.PublisherProfileID, mediaEvents)
			if len(finalRenderRoot.EventID) == 0 {
				log.Printf("correlationID: %s WARN no finalRenderRoot present for publish, pubEvent: %s",
					ledgerItem.LedgerID, p.GetEventID())
				continue
			}
			publishCommand := s.toPublishCommand(p, finalRenderRoot)
			result = append(result, publishCommand)
		}
	}
	return result, err
}

func (s *PublishWorkFlow) isRenderWithoutPublish(root tables.PublishEvent, publishStates map[string]tables.PublishEvent) bool {
	if root.PublishStatus != tables.RENDERING {
		return false
	}

	existingPublishingEvent, ok := publishStates[fmt.Sprintf("%s.%s.%s", root.DistributionChannel,
		root.RootMediaEventID, tables.PUBLISHING)]
	if ok && existingPublishingEvent.ExpiresAtTTL < time.Now().UnixMilli() {
		// Expired, allow append new publish event.
		return true
	}
	return !ok
}

func (s *PublishWorkFlow) getFinalRenderRoot(mediaRootId string, publisherProfileId string,
	mediaEvents []tables.MediaEvent) tables.MediaEvent {
	for _, m := range mediaEvents {
		if m.ParentEventID == mediaRootId && m.IsFinalRender &&
			m.FinalRenderPublisherID == publisherProfileId {
			return m
		}
	}
	return tables.MediaEvent{}
}

func (s *PublishWorkFlow) toPublishCommand(publishEvent tables.PublishEvent,
	finalRenderMedia tables.MediaEvent) drivers.PublishCommand {
	result := drivers.PublishCommand{
		RootPublishEvent:     publishEvent,
		FinalRenderMediaRoot: finalRenderMedia,
	}
	return result
}
