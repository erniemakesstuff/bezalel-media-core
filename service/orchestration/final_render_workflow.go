package orchestration

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"

	dal "github.com/bezalel-media-core/v2/dal"
	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
)

// Creates final-render that compiles child media events.
type FinalRenderWorkflow struct{}

func (s *FinalRenderWorkflow) GetWorkflowName() string {
	return "FinalRenderWorkflow"
}

func (s *FinalRenderWorkflow) Run(ledgerItem tables.Ledger, processId string) error {
	assignedPublishEvents, err := s.getPublishEventsWhereAssigned(ledgerItem)
	if err != nil {
		return err
	}

	rootMediasReadyForPublish, err := s.getRootMediaAllChildrenReady(ledgerItem, assignedPublishEvents)
	if err != nil {
		return err
	}

	err = s.spawnFinalRenderMediaEvent(ledgerItem, rootMediasReadyForPublish)
	return err
}

func (s *FinalRenderWorkflow) getPublishEventsWhereAssigned(ledgerItem tables.Ledger) ([]tables.PublishEvent, error) {
	assignedPublishEvents := []tables.PublishEvent{}
	publishEvents, err := ledgerItem.GetExistingPublishEvents()
	if err != nil {
		log.Printf("correlationID: %s error getting publish events from ledger: %s", ledgerItem.LedgerID, err)
		return assignedPublishEvents, err
	}

	publisherEventMap := CreatePubStateToPublisherMap(publishEvents)

	for _, p := range publishEvents {
		if s.isAssignedWithoutRender(p, publisherEventMap) {
			assignedPublishEvents = append(assignedPublishEvents, p)
		}
	}
	return assignedPublishEvents, nil
}
func (s *FinalRenderWorkflow) isAssignedWithoutRender(event tables.PublishEvent, publisherEventMap map[string]tables.PublishEvent) bool {
	keyStringAssigned := fmt.Sprintf("%s.%s.%s", event.DistributionChannel, event.PublisherProfileID, tables.ASSIGNED)
	keyStringRendering := fmt.Sprintf("%s.%s.%s", event.DistributionChannel, event.PublisherProfileID, tables.RENDERING)
	_, isAssigned := publisherEventMap[keyStringAssigned]
	_, isRendering := publisherEventMap[keyStringRendering]
	return isAssigned && !isRendering
}

func (s *FinalRenderWorkflow) getRootMediaAllChildrenReady(ledgerItem tables.Ledger,
	assignedPublishEvents []tables.PublishEvent) ([]tables.MediaEvent, error) {
	rootMedias := []tables.MediaEvent{}
	mediaEvents, err := ledgerItem.GetExistingMediaEvents()
	if err != nil {
		log.Printf("correlationID: %s error getting media events from ledger: %s", ledgerItem.LedgerID, err)
		return mediaEvents, err
	}
	assignedRootMediaIds := make(map[string]string)
	for _, p := range assignedPublishEvents {
		assignedRootMediaIds[p.RootMediaEventID] = p.GetEventID()
	}
	for _, r := range mediaEvents {
		if _, ok := assignedRootMediaIds[r.GetEventID()]; ok && AllChildrenRendered(r, mediaEvents) {
			rootMedias = append(rootMedias, r)
		}
	}
	return rootMedias, nil
}

func (s *FinalRenderWorkflow) spawnFinalRenderMediaEvent(ledgerItem tables.Ledger, rootMediaEventsToFinalize []tables.MediaEvent) error {
	mediaEvents, err := ledgerItem.GetExistingMediaEvents()
	if err != nil {
		log.Printf("correlationID: %s error getting media events from ledger: %s", ledgerItem.LedgerID, err)
		return err
	}
	publishEvents, err := ledgerItem.GetExistingPublishEvents()
	if err != nil {
		log.Printf("correlationID: %s error getting publish events from ledger: %s", ledgerItem.LedgerID, err)
		return err
	}
	mediaEventToPublisherMap := CreateMediaEventToPublisherMap(publishEvents, mediaEvents)
	for _, r := range rootMediaEventsToFinalize {
		children := CollectChildrenEvents(r, mediaEvents)
		sort.Sort(tables.ByRenderSequence(children))
		assignedPublisherProfile := mediaEventToPublisherMap[r.GetEventID()]
		finalMediaEvent := s.createFinalRenderMediaEventFromChildren(ledgerItem, r, children, assignedPublisherProfile)
		err = HandleMediaGeneration(ledgerItem, finalMediaEvent)
		if err != nil {
			log.Printf("correlationID: %s failed to append finalRender media event: %s", ledgerItem.LedgerID, err)
			return err
		}

		err = dal.AppendLedgerPublishEvents(ledgerItem.LedgerID, []tables.PublishEvent{
			s.createPublishEventRender(assignedPublisherProfile)})
		if err != nil {
			log.Printf("correlationID: %s failed to append RENDERING publish event: %s", ledgerItem.LedgerID, err)
			return err
		}
	}

	return err
}

func (s *FinalRenderWorkflow) createFinalRenderMediaEventFromChildren(
	ledgerItem tables.Ledger, root tables.MediaEvent, children []tables.MediaEvent,
	publishEvent tables.PublishEvent) tables.MediaEvent {
	watermarkText, err := dal.GetPublisherWatermarkInfo(publishEvent.OwnerAccountID, publishEvent.PublisherProfileID)
	if err != nil {
		// non-critical path, continue on failure.
		log.Printf("correlationID: %s WARN failed retrieve watermark text: %s", ledgerItem.LedgerID, err)
	}
	if watermarkText == "" {
		log.Printf("correlationID: %s WARN watermark empty, setting default watermark: TrueVineAI", ledgerItem.LedgerID)
		watermarkText = "TrueVineAI"
	}
	result := tables.MediaEvent{
		Language:           root.Language,
		Niche:              root.Niche,
		MediaType:          tables.RENDER,
		PromptInstruction:  "CREATING FINAL RENDER",
		DistributionFormat: root.DistributionFormat,
		IsFinalRender:      true,
		WatermarkText:      watermarkText,
		ParentEventID:      root.EventID,
	}
	result.PromptHash = tables.HashString(result.PromptInstruction)
	result.EventID = result.GetEventID()
	result.FinalRenderSequences = s.createJsonOfRenderSequence(children)
	result.ContentLookupKey = result.GetContentLookupKey()
	return result
}

func (s *FinalRenderWorkflow) createJsonOfRenderSequence(childrenEvents []tables.MediaEvent) string {
	if len(childrenEvents) == 0 {
		return ""
	}
	renderSequences := []tables.RenderMediaSequence{}
	for _, m := range childrenEvents {
		renderSequences = append(renderSequences, m.ToRenderSequence())
	}
	b, _ := json.Marshal(renderSequences)
	return string(b)
}

func (s *FinalRenderWorkflow) createPublishEventRender(originalEvent tables.PublishEvent) tables.PublishEvent {
	result := originalEvent
	result.PublishStatus = tables.RENDERING
	return result
}
