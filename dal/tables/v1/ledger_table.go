package v1

import (
	"fmt"
)

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
type Event interface {
	GetEventID() string
}

type MediaEvent struct {
	PromptInstruction string // Instructions for the diffusion models. Will be used to vectorize & re-use media. IDEMPOTENT
	MediaType         string // Avatar, Avatar.Custom, Text, Video, ...; used to determine appropriate PGVector table.
	ContentLookupKey  string // GUID into s3: e.g. <MediaType>.<SomeGuid>... Use guid because promptHash for scripts will collide.
	Niche             string
	Language          string
	PromptHash        string // Hash of the prompt instruction
	ParentEventID     string // null for root. Will be set if part of a script ID.
}

func (m *MediaEvent) GetEventID() string {
	// derivable concatenation <Language>.<MediaType>.<Niche>.<PromptInstructionHash>: E.g. EN.LongFormVideo.NewsReport IDEMPOTENT
	return fmt.Sprintf("%s.%s.%s.%s", m.Language, m.MediaType, m.Niche, m.PromptHash)
}

type PublishStatus string

const (
	ASSIGNED   PublishStatus = "ASSIGNED"
	PUBLISHING PublishStatus = "PUBLISHING"
	COMPLETE   PublishStatus = "COMPLETE" // Terminal, success.
	EXPIRED    PublishStatus = "EXPIRED"  // Terminal, failure, timeout.
)

// Associating Script to a PublisherProfile.
type PublishEvent struct {
	ScriptEventID      string        // ContentType --> distribution channel selection.
	PublishStatus      PublishStatus // Soft lock: ASSIGNED, PUBLISHING, COMPLETE, EXPIRED.
	MinutesTTL         int           // Lifetime of assignment lock prior to entering EXPIRED state if no associated COMPLETE.
	PublisherProfileID string
	OwnerAccountID     string
}

func (m *PublishEvent) GetEventID() string {
	// concat <script_Id>.<publisher_profile_id>.<publish_status>
	return fmt.Sprintf("%s.%s.%s", m.ScriptEventID, m.PublisherProfileID, m.PublishStatus)
}
