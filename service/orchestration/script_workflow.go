package orchestration

import (
	"log"

	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
	manifest "github.com/bezalel-media-core/v2/manifest"
)

type ScriptWorkflow struct{}

func (s *ScriptWorkflow) GetWorkflowName() string {
	return "ScriptWorkflow"
}

func (s *ScriptWorkflow) Run(ledgerItem tables.Ledger) error {
	if alreadyScripted(ledgerItem) {
		log.Printf("correlationID: %s ledger already has scripts.", ledgerItem.LedgerID)
		return nil
	}
	prompts := manifest.GetManifestLoader().GetScriptPromptsFromSource(ledgerItem.RawEventSource)
	for _, p := range prompts {
		mediaEvent, err := getMediaEventFromPrompt(p)
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

func alreadyScripted(ledgerItem tables.Ledger) bool {
	// TODO: look through MediaEvents collection to see if Text-prompts already exists.
	return false
}

func getMediaEventFromPrompt(prompt manifest.Prompt) (tables.MediaEvent, error) {
	return tables.MediaEvent{}, nil
}
