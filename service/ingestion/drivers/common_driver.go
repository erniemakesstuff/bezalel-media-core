package drivers

import (
	"crypto/md5"
	"encoding/hex"
	"io"
	"strings"

	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
	"github.com/google/uuid"
)

type Driver interface {
	BuildEventPayload() (tables.Ledger, error)
	IsReady() bool
	WithMedia(io.ReadCloser) error
}

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

func newLedgerFromUrls(targetLanguage string, urls []string, source string) tables.Ledger {
	return tables.Ledger{
		LedgerID:                   uuid.New().String(),
		LedgerStatus:               tables.NEW_LEDGER,
		TriggerEventMediaUrls:      strings.Join(urls, ","),
		TriggerEventSource:         source,
		TriggerEventContentHash:    getMD5Hash(strings.Join(urls, ",")),
		TriggerEventTargetLanguage: targetLanguage, // TODO: Expand ISO Language Code to canonical name.
	}
}

func getMD5Hash(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}
