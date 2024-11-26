package drivers

import (
	"crypto/md5"
	"encoding/hex"

	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
	"github.com/google/uuid"
)

func newLedgerFromText(targetLanguage string, text string, source string) tables.Ledger {
	return tables.Ledger{
		LedgerID:                   uuid.New().String(),
		LedgerStatus:               tables.NEW_LEDGER,
		TriggerEventPayload:        text,
		TriggerEventSource:         source,
		TriggerEventContentHash:    getMD5Hash(text),
		TriggerEventTargetLanguage: targetLanguage, // TODO: Expand ISO Language Code to canonical name.
	}
}

func getMD5Hash(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}
