package orchestration

import (
	"encoding/json"
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
	if len(assignedPublishEvents) == 0 {
		return nil
	}

	rootMediasReadyForPublish, err := s.getRootMediaAllChildrenReady(ledgerItem, assignedPublishEvents)
	if err != nil {
		return err
	}
	if len(rootMediasReadyForPublish) == 0 {
		return nil
	}
	err = s.spawnFinalRenderMediaEvent(ledgerItem, rootMediasReadyForPublish, assignedPublishEvents)
	return err
}

func (s *FinalRenderWorkflow) getPublishEventsWhereAssigned(ledgerItem tables.Ledger) ([]tables.PublishEvent, error) {
	assignedPublishEvents := []tables.PublishEvent{}
	publishEvents, err := ledgerItem.GetExistingPublishEvents()
	if err != nil {
		log.Printf("correlationID: %s error getting publish events from ledger: %s", ledgerItem.LedgerID, err)
		return assignedPublishEvents, err
	}

	publisherEventMap := PubStateByPubEventID(publishEvents)

	for _, p := range publishEvents {
		if s.isAssignedWithoutRender(p, publisherEventMap) {
			assignedPublishEvents = append(assignedPublishEvents, p)
		}
	}
	return assignedPublishEvents, nil
}
func (s *FinalRenderWorkflow) isAssignedWithoutRender(event tables.PublishEvent, publisherEventMap map[string]tables.PublishEvent) bool {
	keyStringAssigned := event.GetEventIDByState(tables.ASSIGNED)
	keyStringRendering := event.GetEventIDByState(tables.RENDERING)
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
		if _, ok := assignedRootMediaIds[r.EventID]; ok && AllChildrenRendered(r.EventID, mediaEvents) {
			rootMedias = append(rootMedias, r)
		}
	}
	return rootMedias, nil
}

func (s *FinalRenderWorkflow) spawnFinalRenderMediaEvent(ledgerItem tables.Ledger, rootMediaEventsToFinalize []tables.MediaEvent,
	assignedPublisherProfiles []tables.PublishEvent) error {
	mediaEvents, err := ledgerItem.GetExistingMediaEvents()
	if err != nil {
		log.Printf("correlationID: %s error getting media events from ledger: %s", ledgerItem.LedgerID, err)
		return err
	}
	mediaEventToPublisherMap := CreateMediaEventToPublisherMap(assignedPublisherProfiles, rootMediaEventsToFinalize)
	for _, r := range rootMediaEventsToFinalize {
		children := CollectChildMediaEligibleForFinalRender(r.EventID, mediaEvents)
		sort.Sort(tables.ByRenderSequence(children))
		assignedPubs, ok := mediaEventToPublisherMap[r.EventID]
		if !ok || len(assignedPubs) == 0 {
			log.Printf("correlationID: %s WARN missing PubState for root media: %s", ledgerItem.LedgerID, r.EventID)
			continue
		}
		finalMediaEvents := s.collectFinalRenderMedia(ledgerItem, r, children, assignedPubs)
		err = HandleMediaGeneration(ledgerItem, finalMediaEvents)
		if err != nil {
			log.Printf("correlationID: %s failed to append finalRender media event: %s", ledgerItem.LedgerID, err)
			return err
		}
		renderEvents := s.createPublishEventRenders(assignedPubs)
		err = dal.AppendLedgerPublishEvents(ledgerItem.LedgerID, renderEvents)
		if err != nil {
			log.Printf("correlationID: %s failed to append RENDERING publish event: %s", ledgerItem.LedgerID, err)
			return err
		}
	}

	return err
}

func (s *FinalRenderWorkflow) collectFinalRenderMedia(
	ledgerItem tables.Ledger, root tables.MediaEvent, children []tables.MediaEvent,
	publishEvents []tables.PublishEvent) []tables.MediaEvent {
	resultCollection := []tables.MediaEvent{}
	for _, p := range publishEvents {
		watermarkText, err := dal.GetPublisherWatermarkInfo(p.AccountID, p.PublisherProfileID)
		if err != nil {
			// non-critical path, continue on failure.
			log.Printf("correlationID: %s WARN failed retrieve watermark text: %s", ledgerItem.LedgerID, err)
		}
		if watermarkText == "" {
			log.Printf("correlationID: %s WARN watermark empty, setting default watermark: TrueVineMedia", ledgerItem.LedgerID)
			watermarkText = "TrueVineMedia"
		}
		result := root.ToMetadataEventEntry(tables.FINAL_RENDER, p.PublisherProfileID, tables.MEDIA_RENDER)
		result.WatermarkText = watermarkText
		result.FinalRenderSequences = s.createJsonOfRenderSequence(root, children)
		resultCollection = append(resultCollection, result)
	}

	return resultCollection
}

func (s *FinalRenderWorkflow) createJsonOfRenderSequence(scriptRoot tables.MediaEvent, childrenEvents []tables.MediaEvent) string {
	// Script root included for blog text (i.e. text content is the final render).
	// TODO: Replace final text body with image/video urls as needed during the final-render consumption process
	// as needed.
	scriptRootMetadata := scriptRoot.ToRenderSequence()
	scriptRootMetadata.PositionLayer = tables.SCRIPT
	scriptRootMetadata.RenderSequence = -1
	renderSequences := []tables.RenderMediaSequence{scriptRootMetadata}
	for _, m := range childrenEvents {
		renderSequences = append(renderSequences, m.ToRenderSequence())
	}
	b, _ := json.Marshal(renderSequences)
	return string(b)
}

func (s *FinalRenderWorkflow) createPublishEventRenders(originalEvents []tables.PublishEvent) []tables.PublishEvent {
	resultCollection := []tables.PublishEvent{}
	for _, o := range originalEvents {
		result := o
		result.PublishStatus = tables.RENDERING
		resultCollection = append(resultCollection, result)
	}

	return resultCollection
}
