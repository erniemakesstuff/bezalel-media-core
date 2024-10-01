package orchestration

import (
	"fmt"
	"log"
	"time"

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
	publishEvents, err := ledgerItem.GetExistingPublishEvents()
	if err != nil {
		log.Printf("correlationID: %s error extracting publish events from ledger item: %s", ledgerItem.LedgerID, err)
		return err
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
		if IsParentMediaEvent(m) && AllChildrenRendered(m, mediaEvents) {
			result = append(result, m)
		}
	}
	return result, nil
}

func (s *AssignmentWorkflow) assignMedia(ledgerItem tables.Ledger, mediaEventsReadyToAssign []tables.MediaEvent,
	publishEvents []tables.PublishEvent, processId string) error {
	// 1. Init map of PublishEvent IDs containing states. Val PublishEvent
	publishEventMap := s.createPublisherMap(publishEvents)
	// 2. For each "ready" media event, collect valid distribution channel names
	// 3. For each media event & channel name target; if absent from PublishEvent map, and not expired; then publish.
	for _, m := range mediaEventsReadyToAssign {
		targetChannelNames := manifest.GetManifestLoader().ChannelNamesFromFormat(string(m.DistributionFormat))
		for _, name := range targetChannelNames {
			if s.isAssignable(m, name, publishEventMap) {
				// Assign.
				err := s.assignMediaToPublisher(ledgerItem, m, name, processId)
				if err != nil {
					log.Printf("correlationID: %s failed to assign media to publisher: %s", ledgerItem.LedgerID, err)
					return err
				}
			}
		}
	}
	return nil
}

func (s *AssignmentWorkflow) createPublisherMap(publishEvents []tables.PublishEvent) map[string]tables.PublishEvent {
	result := make(map[string]tables.PublishEvent)
	if len(publishEvents) == 0 {
		return result
	}
	for _, p := range publishEvents {
		result[p.GetRootMediaAssignmentKey()] = p
	}
	return result
}

func (s *AssignmentWorkflow) isAssignable(mediaEvent tables.MediaEvent, targetChannelName string, publishEventMap map[string]tables.PublishEvent) bool {
	// if unassigned, true
	stateKeyAssigned := fmt.Sprintf("%s.%s.%s", targetChannelName, mediaEvent.GetEventID(), tables.ASSIGNED)
	if _, ok := publishEventMap[stateKeyAssigned]; !ok {
		return true
	}

	stateKeyCompleted := fmt.Sprintf("%s.%s.%s", targetChannelName, mediaEvent.GetEventID(), tables.COMPLETE)
	// if assigned, but already completed
	if _, ok := publishEventMap[stateKeyCompleted]; ok {
		return false
	}

	stateKeyExpired := fmt.Sprintf("%s.%s.%s", targetChannelName, mediaEvent.GetEventID(), tables.EXPIRED)
	// if assigned, but expired, true
	if _, ok := publishEventMap[stateKeyExpired]; ok {
		return true
	}
	return false
}

func (s *AssignmentWorkflow) assignMediaToPublisher(ledgerItem tables.Ledger, mediaEvent tables.MediaEvent,
	distributionChannelName string, processId string) error {
	assignedPublisherProfile, err := dal.AssignOldestActivePublisherProfile(processId, distributionChannelName)
	if err != nil {
		log.Printf("unable to assign media event to publisher profile: %s", err)
		return err
	}
	publishProfileEvent := s.buildPublishEvent(assignedPublisherProfile, mediaEvent, distributionChannelName, processId)
	return dal.AppendLedgerPublishEvents(ledgerItem.LedgerID, []tables.PublishEvent{publishProfileEvent})
}

func (s *AssignmentWorkflow) buildPublishEvent(publisherAccount tables.AccountPublisher,
	mediaEvent tables.MediaEvent,
	distributionChannelName string, processId string) tables.PublishEvent {

	const ninetyMinutes = 5400000 // TODO: Replace w/ env config
	expiryAtTime := time.Now().UnixMilli() + ninetyMinutes
	return tables.PublishEvent{
		DistributionChannel: distributionChannelName,
		ProcessOwner:        processId,
		ExpiresAtTTL:        expiryAtTime,
		PublishStatus:       tables.ASSIGNED,
		PublisherProfileID:  publisherAccount.PublisherProfileID,
		OwnerAccountID:      publisherAccount.AccountID,
		RootMediaEvent:      mediaEvent.GetEventID(),
	}
}
