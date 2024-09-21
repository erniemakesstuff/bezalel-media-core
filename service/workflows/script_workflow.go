package workflows

import (
	"log"

	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
)

func ProcessScripts(ledgerItem tables.Ledger) error {
	if alreadyScripted(ledgerItem) {
		log.Printf("correlationID: %s ledger already has scripts.", ledgerItem.LedgerID)
		return nil
	}
	return nil
}

func alreadyScripted(ledgerItem tables.Ledger) bool {
	if ledgerItem.ScriptEvents == "" || len(ledgerItem.ScriptEvents) == 0 {
		return false
	}
	return true
}
