package v1

type LedgerStatus string

const (
	NEW_LEDGER      LedgerStatus = "NEW"
	FINISHED_LEDGER LedgerStatus = "FINISHED" // Terminal
)

type Ledger struct {
	// Required
	LedgerID                  string       // Also system correlation ID.
	LedgerStatus              LedgerStatus // Directional status towards terminus.
	LedgerCreatedAtEpochMilli int64        // CreatedAt for replayability

	// Optional
	RawEventPayload      string // article text, ...
	RawEventSource       string
	RawEventMediaUrls    string // Images, videos, ...
	RawEventWebsiteUrls  string // product pages, news article sources crawled
	RawEventLanguage     string // EN, CN, ...
	RawContentHash       string // for deduping raw events.
	MediaEvents          string
	ScriptEvents         string
	PublishEvents        string
	MediaEventsVersion   int64
	ScriptEventsVersion  int64
	PublishEventsVersion int64
}

type ScriptEvent struct {
	Foo string
}
