package orchestration

import (
	"log"
	"strings"

	dao "github.com/bezalel-media-core/v2/dal"
	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
	manifest "github.com/bezalel-media-core/v2/manifest"
)

type ScriptWorkflow struct{}

const scriptMediaType = "Text"

func (s *ScriptWorkflow) GetWorkflowName() string {
	return "ScriptWorkflow"
}

func (s *ScriptWorkflow) Run(ledgerItem tables.Ledger) error {
	prompts := manifest.GetManifestLoader().GetScriptPromptsFromSource(ledgerItem.RawEventSource)
	existingMediaEvents, err := dao.GetExistingMediaEvents(ledgerItem)
	if err != nil {
		log.Printf("correlationID: %s error deserializing existing media events from ledger: %s", ledgerItem.LedgerID, err)
		return err
	}
	for _, p := range prompts {
		mediaEvent, err := getMediaEventFromPrompt(p, ledgerItem)
		if isAlreadyScripted(existingMediaEvents, mediaEvent) {
			continue
		}

		if err != nil {
			log.Printf("correlationID: %s failed to get media event from prompt: %s", ledgerItem.LedgerID, err)
			return err
		}
		err = HandleMediaGeneration(ledgerItem, mediaEvent)
		if err != nil {
			log.Printf("correlationID: %s failed to handle media generation for script workflow: %s", ledgerItem.LedgerID, err)
			return err
		}
	}
	return nil
}

func isAlreadyScripted(existingMediaEvents []tables.MediaEvent, mediaEvent tables.MediaEvent) bool {
	for _, m := range existingMediaEvents {
		if m.EventID == mediaEvent.GetEventID() {
			return true
		}
	}
	return false
}

func getMediaEventFromPrompt(prompt manifest.Prompt, ledgerItem tables.Ledger) (tables.MediaEvent, error) {
	result := tables.MediaEvent{}
	result.SystemPromptInstruction = prompt.SystemPromptText
	result.MediaType = scriptMediaType
	result.Niche = prompt.GetNiche()
	result.Language = prompt.GetLanguage()
	var err error
	result.DistributionFormat, err = tables.GetDistributionFormatFromString(
		prompt.GetDistributionFormat())
	if err != nil {
		log.Printf("correlationID: %s Mismatched distribution format, %s", ledgerItem.LedgerID, err)
		return result, err
	}

	// TODO: Replace w/ raws; variable replacement.
	enrichedPrompt := strings.Replace(prompt.PromptText, manifest.PROMPT_SCRIPT_VAR_RAW_TEXT, ledgerItem.RawEventPayload, -1)
	result.PromptInstruction = enrichedPrompt
	result.PromptHash = tables.HashString(result.PromptInstruction)
	result.EventID = result.GetEventID()
	result.ContentLookupKey = result.GetContentLookupKey()
	return result, nil
}
