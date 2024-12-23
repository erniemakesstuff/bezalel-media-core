package orchestration

import (
	"fmt"
	"log"
	"strings"

	"github.com/bezalel-media-core/v2/dal"
	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
	"github.com/bezalel-media-core/v2/manifest"
	drivers "github.com/bezalel-media-core/v2/service/orchestration/publisher-drivers"
)

// Add child events based on script-output of Parent Media Event.
type EnrichmentWorkflow struct{}

func (s *EnrichmentWorkflow) GetWorkflowName() string {
	return "EnrichmentWorkflow"
}

func (s *EnrichmentWorkflow) Run(ledgerItem tables.Ledger, processId string) error {
	// TODO: Support images for parent event.
	// TODO: Set position layer, and render sequence
	//		Audio and Video can have the same RenderSequence if they are concurrent, or the media template allows (e.g. splitscreen)
	mediaEvents, err := ledgerItem.GetExistingMediaEvents()
	if err != nil {
		log.Printf("correlationID: %s error extracting media events from ledger item: %s", ledgerItem.LedgerID, err)
		return err
	}

	for _, parentMedia := range mediaEvents {
		if !IsParentMediaEvent(parentMedia) {
			continue
		}

		exists, err := MediaExists(parentMedia.ContentLookupKey)
		if err != nil {
			log.Printf("correlationID: %s error enrichment cannot determine media existence: %s", ledgerItem.LedgerID, err)
			return err
		}

		if !exists {
			continue
		}

		textPayload, err := drivers.LoadAsString(parentMedia.ContentLookupKey)
		if err != nil {
			log.Printf("correlationID: %s failed to load media as string from enricher: %s", ledgerItem.LedgerID, err)
			return err
		}

		if strings.Contains(textPayload, "EDITOR_FORBIDDEN") {
			log.Printf("correlationID: %s detected forbidden media, marking workflow as finished.", ledgerItem.LedgerID)
			return dal.SetLedgerStatus(ledgerItem, tables.FINISHED_LEDGER)
		}

		err = spawnChildMediaEvents(ledgerItem, parentMedia, mediaEvents)
		if err != nil {
			log.Printf("correlationID: %s failed to spawn child media events: %s", ledgerItem.LedgerID, err)
			return err
		}
	}

	return err
}

func spawnChildMediaEvents(ledgerItem tables.Ledger, parentMediaEvent tables.MediaEvent, existingMediaEvents []tables.MediaEvent) error {
	childEvents, err := buildEventsByDistFormat(parentMediaEvent, existingMediaEvents)
	if err != nil {
		return err
	}

	// Metadata entry to indicate enrichment finished.
	enrichedEntry := parentMediaEvent.ToMetadataEventEntry(tables.SCRIPT_ENRICHED, parentMediaEvent.RestrictToPublisherID, parentMediaEvent.MediaType)
	childEvents = append(childEvents, enrichedEntry)
	return HandleMediaGeneration(ledgerItem, childEvents)
}

func buildEventsByDistFormat(parentMediaEvent tables.MediaEvent, existingMediaEvents []tables.MediaEvent) ([]tables.MediaEvent, error) {
	distForm := parentMediaEvent.DistributionFormat
	jsonPayload, err := drivers.LoadAsString(parentMediaEvent.ContentLookupKey)
	if err != nil {
		return []tables.MediaEvent{}, err
	}
	if manifest.DIST_FORMAT_BLOG == distForm || manifest.DIST_FORMAT_INTEG_BLOG == distForm {
		return enrichBlog(jsonPayload, parentMediaEvent, existingMediaEvents)
	} else if manifest.DIST_FORMAT_TINY_BLOG == distForm {
		return enrichTinyBlog(jsonPayload, parentMediaEvent, existingMediaEvents)
	} else if manifest.DIST_FORMAT_SHORT_VIDEO == distForm {
		return enrichShortVideo(jsonPayload, parentMediaEvent, existingMediaEvents)
	}
	return []tables.MediaEvent{}, fmt.Errorf("no matching enrichment process for distributionFormat: %s", distForm)
}

func enrichBlog(jsonPayload string, parentMediaEvent tables.MediaEvent, existingMediaEvents []tables.MediaEvent) ([]tables.MediaEvent, error) {
	events := []tables.MediaEvent{}

	schemaResult, err := drivers.ScriptPayloadToBlogSchema(jsonPayload)
	if err != nil {
		return events, err
	}
	// TODO: Perform other enrichment activities for blogs here.
	return createBlogChildEventsFromImageDescriptions(schemaResult.ImageDescriptionTexts, parentMediaEvent, existingMediaEvents), nil
}

func enrichTinyBlog(jsonPayload string, parentMediaEvent tables.MediaEvent, existingMediaEvents []tables.MediaEvent) ([]tables.MediaEvent, error) {
	events := []tables.MediaEvent{}
	schemaResult, err := drivers.ScriptPayloadToTinyBlogSchema(jsonPayload)
	if err != nil {
		return events, err
	}
	// TODO: perform other enrichment activities for tiny blogs here.
	return createBlogChildEventsFromImageDescriptions(schemaResult.ImageDescriptionTexts, parentMediaEvent, existingMediaEvents), nil
}

func enrichShortVideo(jsonPayload string, parentMediaEvent tables.MediaEvent, existingMediaEvents []tables.MediaEvent) ([]tables.MediaEvent, error) {
	events := []tables.MediaEvent{}
	schemaResult, err := drivers.ScriptPayloadToShortVideoSchema(jsonPayload)
	if err != nil {
		return events, err
	}

	return createShortVideoChildEvents(schemaResult, parentMediaEvent, existingMediaEvents), err
}
