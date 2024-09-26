package v1

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
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
	PublishEvents        string // Publish to distribution channel: YouTube, Instagram, ...
	MediaEventsVersion   int64
	PublishEventsVersion int64
}
type Event interface {
	GetEventID() string
}

// MediaType determine what downstream media-generator will be used for this MediaEvent.
type MediaType string

const (
	MEDIA_TEXT MediaType = "Text"
)

// DistributionFormat are only set for the Parent/Root MediaEvent.
// Used to select the applicable downstream PublisherProfile that supports the format.
// E.g. You cannot publish a Blog to Snapchat, but you can publish a Blog to Medium or Reddit.
type DistributionFormat string

const (
	DIST_FORMAT_BLOG   DistributionFormat = "Blog"
	DIST_FORMAT_LVIDEO DistributionFormat = "LongformVideo"
)

type MediaEvent struct {
	PromptInstruction       string             // Instructions for the diffusion models. Will be used to vectorize & re-use media.
	SystemPromptInstruction string             // Roles, personalities, or response guidelines for the LLM.
	MediaType               MediaType          // Avatar, Avatar.Custom, Text, Video, ...; used to determine appropriate PGVector table.
	DistributionFormat      DistributionFormat // LongFormVideo, ShortFormVideo, Image, Blog, ...
	ContentLookupKey        string             // GUID into s3: e.g. <MediaType>.<SomeGuid>...
	Niche                   string
	Language                string
	PromptHash              string // Hash of the prompt instruction
	EventID                 string // Although derivable GetEventID, set for convenience on downstream calls.
	ParentEventID           string // null for root. Will be set if part of a script ID.
}

func GetDistributionFormatFromString(format string) (DistributionFormat, error) {
	switch {
	case strings.EqualFold(format, string(DIST_FORMAT_BLOG)):
		return DIST_FORMAT_BLOG, nil
	case strings.EqualFold(format, string(DIST_FORMAT_LVIDEO)):
		return DIST_FORMAT_LVIDEO, nil
	}
	return DIST_FORMAT_BLOG, errors.New("unable to find matching distribution format from string")
}

func (m *MediaEvent) GetEventID() string {
	// derivable concatenation <Language>.<MediaType>.<Niche>.<PromptInstructionHash>: E.g. EN.LongFormVideo.NewsReport....
	// Enforce idempotency within the context of a ledger entry; no datastore collision.
	return fmt.Sprintf("%s.%s.%s.%s", m.Language, m.MediaType, m.Niche, m.PromptHash)
}

func (m *MediaEvent) GetContentLookupKey() string {
	// Use guid because promptHash for static-scripts will collide.
	return fmt.Sprintf("%s.%s", m.MediaType, uuid.New().String())
}

func HashString(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

type PublishStatus string

const (
	ASSIGNED   PublishStatus = "ASSIGNED"
	PUBLISHING PublishStatus = "PUBLISHING"
	COMPLETE   PublishStatus = "COMPLETE" // Terminal, success.
	EXPIRED    PublishStatus = "EXPIRED"  // Terminal, failure, timeout.
)

// Associating Script to a PublisherProfile. Used for softlocking.
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
