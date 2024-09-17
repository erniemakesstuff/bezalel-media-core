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
	MediaEvents          string // Media generation: audio, video, ...
	ScriptEvents         string // Text generation.
	PublishEvents        string // Publish to distribution channel: YouTube, Instagram, ...
	MediaEventsVersion   int64
	ScriptEventsVersion  int64
	PublishEventsVersion int64
}

type ScriptEvent struct {
	ScriptEventID    string // derivable concatenation <Language>.<ContentType>.<Niche>: E.g. EN.LongFormVideo.NewsReport IDEMPOTENT
	ContentLookupKey string // some guid to fetch into s3. Namespace by event: e.g. script.1029-102S-1290AKXL
}

type MediaEvent struct {
	PromptInstruction string // Instructions for the diffusion models. Will be used to vectorize & re-use media. IDEMPOTENT
	MediaType         string // Avatar, Avatar.Custom, BRoll, Text, Video, Music, Voice, ...
	ScriptEventID     string // Media associated to script. Many-Many.
	ContentLookupKey  string // GUID into s3: e.g. media.XXXX-XXXX...
}

// Associating Script to a PublisherProfile.
type PublishEvent struct {
	ScriptEventID      string // ContentType --> distribution channel selection.
	Status             string // Soft lock: ASSIGNED, PUBLISHING, COMPLETE, EXPIRED.
	MinutesTTL         int    // Lifetime of assignment lock prior to entering EXPIRED state if no associated COMPLETE.
	PublisherProfileID string
	OwnerAccountID     string
}
