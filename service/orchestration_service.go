package service

import (
	"log"

	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
)

func ProcessWorkflow(ledgerItem tables.Ledger) error {
	if isCompleteWorkflow(ledgerItem) {
		return nil
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
