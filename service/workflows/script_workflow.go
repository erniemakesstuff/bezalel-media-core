package workflows

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
	// TODO
	manifest.GetManifestLoader().GetScriptPromptsFromSource(ledgerItem.RawEventSource)
	return nil
}

func alreadyScripted(ledgerItem tables.Ledger) bool {
	if ledgerItem.ScriptEvents == "" || len(ledgerItem.ScriptEvents) == 0 {
		return false
	}
	return true
}
