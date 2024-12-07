package orchestration

import (
	"log"
	"time"

	env "github.com/bezalel-media-core/v2/configuration"
	dal "github.com/bezalel-media-core/v2/dal"
	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
	"github.com/bezalel-media-core/v2/manifest"
)

type AssignmentWorkflow struct{}

func (s *AssignmentWorkflow) GetWorkflowName() string {
	return "AssignmentWorkflow"
}

func (s *AssignmentWorkflow) Run(ledgerItem tables.Ledger, processId string) error {
	mediaEvents, err := ledgerItem.GetExistingMediaEvents()
	if err != nil {
		log.Printf("correlationID: %s error extracting media events from ledger item: %s", ledgerItem.LedgerID, err)
		return err
	}
	if len(mediaEvents) == 0 {
		log.Printf("correlationID: %s no media events found", ledgerItem.LedgerID)
	}

	publishEvents, err := ledgerItem.GetExistingPublishEvents()
	if err != nil {
		log.Printf("correlationID: %s error extracting publish events from ledger item: %s", ledgerItem.LedgerID, err)
		return err
	}
	if len(publishEvents) == 0 {
		log.Printf("correlationID: %s no publish events found", ledgerItem.LedgerID)
	}
	mediaEventsReadyToAssign, err := s.collectRootMediaReadyToPublish(mediaEvents)
	if err != nil {
		log.Printf("correlationID: %s error collecting media events to assign, item: %s", ledgerItem.LedgerID, err)
		return err
	}

	err = s.assignMedia(ledgerItem, mediaEventsReadyToAssign, publishEvents, processId)
	return err
}

func (s *AssignmentWorkflow) collectRootMediaReadyToPublish(mediaEvents []tables.MediaEvent) ([]tables.MediaEvent, error) {
	result := []tables.MediaEvent{}
	for _, m := range mediaEvents {
		isEnriched := s.isEnriched(m, mediaEvents)
		if IsParentMediaEvent(m) && AllChildrenRendered(m.EventID, mediaEvents) && isEnriched {
			result = append(result, m)
		}
	}
	return result, nil
}

func (s *AssignmentWorkflow) isEnriched(root tables.MediaEvent, mediaEvents []tables.MediaEvent) bool {
	for _, m := range mediaEvents {
		if m.ParentEventID == root.EventID && m.MetaMediaDescriptor == tables.SCRIPT_ENRICHED {
			return true
		}
	}
	return false
}

func (s *AssignmentWorkflow) assignMedia(ledgerItem tables.Ledger, mediaEventsReadyToAssign []tables.MediaEvent,
	publishEvents []tables.PublishEvent, processId string) error {
	rootMediaStateToPublishEventMap := PubStateByRootMedia(publishEvents)
	publishEventIdToPublishEventMap := PubStateByPubEventID(publishEvents)
	for _, m := range mediaEventsReadyToAssign {
		targetChannelNames := manifest.GetManifestLoader().ChannelNamesFromFormat(string(m.DistributionFormat))
		if len(targetChannelNames) == 0 {
			log.Printf("correlationID: %s WARN no target channel names found for distribution format %s",
				ledgerItem.LedgerID, m.DistributionFormat)
		}

		for _, name := range targetChannelNames {
			if s.isAssignable(m, name, rootMediaStateToPublishEventMap, publishEventIdToPublishEventMap, publishEvents) {
				err := s.assignMediaToPublisher(ledgerItem, m, name, processId)
				if err != nil {
					log.Printf("correlationID: %s unable to assign media to publisher: %s", ledgerItem.LedgerID, err)
					return err
				}
			}
		}
	}
	return nil
}

func (s *AssignmentWorkflow) isAssignable(mediaEvent tables.MediaEvent, targetChannelName string,
	rootStateToPublishEventMap map[string]tables.PublishEvent, publishEventIdToPublishEventMap map[string]tables.PublishEvent,
	publishEvents []tables.PublishEvent) bool {
	// if unassigned
	stateKeyAssigned := tables.RootMediaKey(targetChannelName, mediaEvent.EventID, tables.ASSIGNED)
	if _, ok := rootStateToPublishEventMap[stateKeyAssigned]; !ok {
		return true
	}

	stateKeyCompleted := tables.RootMediaKey(targetChannelName, mediaEvent.EventID, tables.COMPLETE)
	// if assigned, but already completed: cannot assign to distribution channel
	if _, ok := rootStateToPublishEventMap[stateKeyCompleted]; ok {
		return false
	}

	// check if no open-assignments
	for _, p := range publishEvents {
		if p.PublishStatus != tables.ASSIGNED || p.RootMediaEventID != mediaEvent.EventID {
			continue
		}
		pubStateExpired := p.GetEventIDByState(tables.EXPIRED)
		pubStateComplete := p.GetEventIDByState(tables.COMPLETE)
		_, hasExpiry := publishEventIdToPublishEventMap[pubStateExpired]
		_, hasComplete := publishEventIdToPublishEventMap[pubStateComplete]
		if !hasExpiry && !hasComplete {
			return false
		}
	}

	stateKeyExpired := tables.RootMediaKey(targetChannelName, mediaEvent.EventID, tables.EXPIRED)
	// if no open assignments, and not complete, check if expired recorded
	if _, ok := rootStateToPublishEventMap[stateKeyExpired]; ok {
		return true
	}

	return false
}

func (s *AssignmentWorkflow) assignMediaToPublisher(ledgerItem tables.Ledger, mediaEvent tables.MediaEvent,
	distributionChannelName string, processId string) error {
	assignedPublisherProfile, err := dal.AssignPublisherProfile(processId, distributionChannelName, mediaEvent.Language, mediaEvent.Niche)
	if err != nil {
		log.Printf("unable to assign media event to publisher profile: %s", err)
		return err
	}
	publishProfileEvent := s.buildPublishEvent(ledgerItem.LedgerID,
		assignedPublisherProfile, mediaEvent, distributionChannelName, processId)
	err = dal.AppendLedgerPublishEvents(ledgerItem.LedgerID, []tables.PublishEvent{publishProfileEvent})
	if err != nil {
		log.Printf("unable to write publish-event to ledger: %s", err)
		// Try release assignment
		dal.ReleaseAssignment(assignedPublisherProfile.AccountID, assignedPublisherProfile.PublisherProfileID, processId)
		return err
	}

	isSuccessfulPublishOwner, err := WaitOptimisticVerifyWroteLedger(publishProfileEvent.GetEventID(), ledgerItem.LedgerID)
	if err != nil || !isSuccessfulPublishOwner {
		log.Printf("unable to verify publish-event ledger ownership: %s", err)
		// Try release assignment
		dal.ReleaseAssignment(assignedPublisherProfile.AccountID, assignedPublisherProfile.PublisherProfileID, processId)
		return err
	}

	return err
}

func (s *AssignmentWorkflow) buildPublishEvent(ledgerId string, publisherAccount tables.AccountPublisher,
	mediaEvent tables.MediaEvent,
	distributionChannelName string, processId string) tables.PublishEvent {

	expiryAtTime := time.Now().UnixMilli() + env.GetEnvConfigs().PublishLockMilliTTL
	return tables.PublishEvent{
		LedgerID:            ledgerId,
		DistributionChannel: distributionChannelName,
		ProcessOwner:        processId,
		ExpiresAtTTL:        expiryAtTime,
		PublishStatus:       tables.ASSIGNED,
		PublisherProfileID:  publisherAccount.PublisherProfileID,
		AccountID:           publisherAccount.AccountID,
		RootMediaEventID:    mediaEvent.EventID,
	}
}
