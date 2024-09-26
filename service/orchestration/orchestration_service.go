package orchestration

import (
	"log"

	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
)

type Workflow interface {
	GetWorkflowName() string
	Run(tables.Ledger) error
}

// TODO: Add workflows in-order here.
// Workflows run in order.
var workflowsToRun = []Workflow{
	&ScriptWorkflow{},
}

func RunWorkflows(ledgerItem tables.Ledger) error {
	if isCompleteWorkflow(ledgerItem) {
		return nil
	}
	for _, w := range workflowsToRun {
		log.Printf("correlationID: %s running %s", ledgerItem.LedgerID, w.GetWorkflowName())
		err := w.Run(ledgerItem)
		if err != nil {
			log.Printf("correlationID: %s workflow %s failed: %s", ledgerItem.LedgerID, w.GetWorkflowName(), err)
		}
	}
	return nil
}

func isCompleteWorkflow(ledgerItem tables.Ledger) bool {
	if ledgerItem.LedgerStatus == tables.FINISHED_LEDGER {
		log.Printf("correlationID: %s ledger finished.", ledgerItem.LedgerID)
		return true
	}
	return false
}
