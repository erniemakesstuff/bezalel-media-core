package orchestration

import (
	"fmt"
	"log"
	"strings"

	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
	manifest "github.com/bezalel-media-core/v2/manifest"
)

type ScriptWorkflow struct{}

const scriptMediaType = "Text"

func (s *ScriptWorkflow) GetWorkflowName() string {
	return "ScriptWorkflow"
}

func (s *ScriptWorkflow) Run(ledgerItem tables.Ledger, processId string) error {
	prompts := manifest.GetManifestLoader().GetScriptPromptsFromSource(ledgerItem.TriggerEventSource)
	if len(prompts) == 0 {
		return fmt.Errorf("correlationID: %s error no prompts received from source: %s", ledgerItem.LedgerID, ledgerItem.TriggerEventSource)
	}
	mediaEventsToRender := []tables.MediaEvent{}
	for _, p := range prompts {
		mediaEvent, err := getMediaEventFromPrompt(p, ledgerItem)
		if err != nil {
			log.Printf("correlationID: %s failed to get media event from prompt: %s", ledgerItem.LedgerID, err)
			return err
		}
		alreadyExists, err := ExistsInLedger(ledgerItem, []tables.MediaEvent{mediaEvent})
		if err != nil {
			log.Printf("correlationID: %s failed to determine ledger existence: %s", ledgerItem.LedgerID, err)
			return err
		}
		if alreadyExists {
			continue
		}
		mediaEventsToRender = append(mediaEventsToRender, mediaEvent)
	}

	err := HandleMediaGeneration(ledgerItem, mediaEventsToRender)
	if err != nil {
		log.Printf("correlationID: %s failed to handle media generation for script workflow: %s", ledgerItem.LedgerID, err)
		return err
	}
	return nil
}

func getMediaEventFromPrompt(prompt manifest.Prompt, ledgerItem tables.Ledger) (tables.MediaEvent, error) {
	result := tables.MediaEvent{}
	result.LedgerID = ledgerItem.LedgerID
	result.SystemPromptInstruction = prompt.SystemPromptText
	result.MediaType = scriptMediaType
	result.Niche = prompt.GetNiche()
	result.Language = ledgerItem.TriggerEventLanguage
	var err error
	result.DistributionFormat, err = tables.GetDistributionFormatFromString(
		prompt.GetDistributionFormat())
	if err != nil {
		log.Printf("correlationID: %s Mismatched distribution format, %s", ledgerItem.LedgerID, err)
		return result, err
	}

	// TODO: Replace w/ raws; variable replacement.
	enrichedPrompt := strings.Replace(prompt.PromptText, manifest.PROMPT_SCRIPT_VAR_RAW_TEXT, ledgerItem.TriggerEventPayload, -1)
	enrichedPrompt = strings.Replace(enrichedPrompt, manifest.PROMPT_SCRIPT_VAR_LANGUAGE, ledgerItem.TriggerEventLanguage, -1)
	result.PromptInstruction = enrichedPrompt
	result.PromptHash = tables.HashString(result.PromptInstruction)
	result.EventID = result.GetEventID()
	result.ContentLookupKey = result.GetContentLookupKey()
	return result, nil
}
