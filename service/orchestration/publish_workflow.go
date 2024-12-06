package orchestration

import (
	"fmt"
	"log"
	"strings"
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

	for _, p := range publishCommands {
		err = s.handlePublish(p, ledgerItem.LedgerID, processId)
	}
	return err
}

func (s *PublishWorkFlow) handlePublish(pubCommand drivers.PublishCommand, ledgerId string, processId string) error {
	_, err := drivers.GetDriver(pubCommand.RootPublishEvent.DistributionChannel)
	if err != nil {
		log.Printf("correlationID: %s error fetching driver: %s", ledgerId, err)
		return err
	}

	err = dal.TakePublishLock(pubCommand.RootPublishEvent.AccountID, pubCommand.RootPublishEvent.PublisherProfileID, processId)
	if err != nil {
		log.Printf("correlationID: %s error taking publisher lock: %s", ledgerId, err)
		return err
	}

	renderEvent := pubCommand.RootPublishEvent
	renderEvent.ProcessOwner = processId
	renderEvent.PublishStatus = tables.PUBLISHING
	err = dal.AppendLedgerPublishEvents(ledgerId, []tables.PublishEvent{renderEvent})
	if err != nil {
		log.Printf("correlationID: %s error appending publisher publishing-event to ledger: %s", ledgerId, err)
		// Try release publish lock
		dal.ReleasePublishLock(pubCommand.RootPublishEvent.AccountID, pubCommand.RootPublishEvent.PublisherProfileID, processId)
		return err
	}

	isSuccessfullyLocked, err := WaitOptimisticVerifyWroteLedger(renderEvent.GetEventID(), ledgerId)
	if err != nil || !isSuccessfullyLocked {
		log.Printf("correlationID: %s unable to verify publish-event ledger softlock: %s", ledgerId, err)
		// Try release publish lock
		dal.ReleasePublishLock(pubCommand.RootPublishEvent.AccountID, pubCommand.RootPublishEvent.PublisherProfileID, processId)
		return err
	}
	/* TODO: Uncomment after local prompt testing.
	err = driver.Publish(pubCommand)
	if err != nil {
		log.Printf("correlationID: %s error publishing: %s", ledgerId, err)
		s.handleBadRequestCode(err, ledgerId, pubCommand.RootPublishEvent)
		// Try release publish lock
		dal.ReleasePublishLock(pubCommand.RootPublishEvent.AccountID, pubCommand.RootPublishEvent.PublisherProfileID, processId)
		return err
	}
	*/

	completionEventRecord := pubCommand.RootPublishEvent
	completionEventRecord.PublishStatus = tables.COMPLETE
	err = dal.AppendLedgerPublishEvents(ledgerId, []tables.PublishEvent{completionEventRecord})
	if err != nil {
		log.Printf("correlationID: %s error appending completion publish event: %s", ledgerId, err)
		return err
	}

	err = dal.RecordPublishTime(pubCommand.RootPublishEvent.AccountID, pubCommand.RootPublishEvent.PublisherProfileID)
	if err != nil {
		log.Printf("correlationID: %s error recording last publish time: %s", ledgerId, err)
		return err
	}

	err = dal.ForceAllLocksFree(pubCommand.RootPublishEvent.AccountID, pubCommand.RootPublishEvent.PublisherProfileID)
	if err != nil {
		log.Printf("correlationID: %s error releasing all locks for successful publish: %s", ledgerId, err)
		return err
	}

	return err
}

func (s *PublishWorkFlow) handleBadRequestCode(err error, ledgerId string, pubEvent tables.PublishEvent) {
	if !strings.Contains(fmt.Sprintf("%s", err), drivers.BAD_REQUEST_PROFILE_CODE) {
		return
	}
	log.Printf("correlationID: %s received bad request from drivers - marking profile stale and descheduling: %s %s",
		ledgerId, pubEvent.AccountID, pubEvent.PublisherProfileID)
	expiredEvent := pubEvent
	expiredEvent.PublishStatus = tables.EXPIRED
	dal.AppendLedgerPublishEvents(ledgerId, []tables.PublishEvent{expiredEvent})
	dal.SetProfileStaleFlag(pubEvent.AccountID, pubEvent.PublisherProfileID, true)
	dal.ForceAllLocksFree(pubEvent.AccountID, pubEvent.PublisherProfileID)
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
		shouldCreatePublishEvent, err := s.isRenderWithoutPublish(p, publishStateToPubMap)
		if err != nil {
			return []drivers.PublishCommand{}, err
		}

		if !shouldCreatePublishEvent || s.isRenderAlreadyCompleted(p, publishStateToPubMap) {
			continue
		}

		childrenMediaEvents := CollectNonMetaChildMedia(p.RootMediaEventID, mediaEvents)
		if len(childrenMediaEvents) == 0 {
			continue
		}

		if AllChildrenRendered(p.RootMediaEventID, childrenMediaEvents) {
			finalRenderEvent := s.getFinalRenderEvent(p.RootMediaEventID, p.PublisherProfileID, childrenMediaEvents)
			scriptMediaEvent := s.getMediaEventById(ledgerItem.LedgerID, finalRenderEvent.ParentEventID, mediaEvents)
			if len(finalRenderEvent.EventID) == 0 {
				log.Printf("correlationID: %s WARN no finalRenderRoot present for publish, pubEvent: %s",
					ledgerItem.LedgerID, p.GetEventID())
				continue
			}
			publishCommand := s.toPublishCommand(p, finalRenderEvent, scriptMediaEvent)
			result = append(result, publishCommand)
		}
	}
	return result, err
}

func (s *PublishWorkFlow) getMediaEventById(ledgerId string, id string, mediaEvents []tables.MediaEvent) tables.MediaEvent {
	tmp := tables.MediaEvent{}
	for _, m := range mediaEvents {
		if m.EventID == id {
			return m
		}
	}
	log.Printf("correlationID: %s WARN no mediaEvent found by Id in mediaEvents: %s",
		ledgerId, id)
	return tmp
}

func (s *PublishWorkFlow) isRenderWithoutPublish(root tables.PublishEvent, publishStates map[string]tables.PublishEvent) (bool, error) {
	if root.PublishStatus != tables.RENDERING {
		return false, nil
	}

	existingPublishingEvent, ok := publishStates[fmt.Sprintf("%s.%s.%s", root.DistributionChannel,
		root.RootMediaEventID, tables.PUBLISHING)]
	if ok && existingPublishingEvent.ExpiresAtTTL < time.Now().UnixMilli() {
		// Expired, allow append new publish event.
		return true, nil
	}
	if !ok {
		return true, nil
	}

	// check that publish is still holding the profile-publish lock, otherwise retry by creating a new pub-event
	pubAccount, err := dal.GetPublisherAccount(existingPublishingEvent.AccountID, existingPublishingEvent.PublisherProfileID)
	if err != nil {
		log.Printf("correlationID: %s error retrieving publisher account within isRenderWithoutPublish: %s",
			existingPublishingEvent.LedgerID, err)
		return false, err
	}

	if len(pubAccount.PublishLockID) == 0 || pubAccount.PublishLockTTL < time.Now().UnixMilli() {
		return true, nil
	}

	return !ok, nil
}

func (s *PublishWorkFlow) isRenderAlreadyCompleted(root tables.PublishEvent, publishStates map[string]tables.PublishEvent) bool {
	if root.PublishStatus != tables.RENDERING {
		return false
	}

	_, isComplete := publishStates[fmt.Sprintf("%s.%s.%s", root.DistributionChannel,
		root.RootMediaEventID, tables.COMPLETE)]
	_, isExpired := publishStates[fmt.Sprintf("%s.%s.%s", root.DistributionChannel,
		root.RootMediaEventID, tables.EXPIRED)]
	return isComplete || isExpired
}

func (s *PublishWorkFlow) getFinalRenderEvent(mediaRootId string, publisherProfileId string,
	mediaEvents []tables.MediaEvent) tables.MediaEvent {
	for _, m := range mediaEvents {
		if m.ParentEventID == mediaRootId && m.MetaMediaDescriptor == tables.FINAL_RENDER &&
			m.RestrictToPublisherID == publisherProfileId {
			return m
		}
	}
	return tables.MediaEvent{}
}

func (s *PublishWorkFlow) toPublishCommand(publishEvent tables.PublishEvent,
	finalRenderMedia tables.MediaEvent, originalScriptMedia tables.MediaEvent) drivers.PublishCommand {
	result := drivers.PublishCommand{
		RootPublishEvent: publishEvent,
		FinalRenderMedia: finalRenderMedia,
		ScriptMedia:      originalScriptMedia,
	}
	return result
}
