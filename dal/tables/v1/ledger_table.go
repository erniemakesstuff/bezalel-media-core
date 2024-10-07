package v1

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/google/uuid"
)

type LedgerStatus string

const (
	NEW_LEDGER      LedgerStatus = "NEW"
	FINISHED_LEDGER LedgerStatus = "FINISHED" // Terminal for all cases: expired or success.
)

type Ledger struct {
	// Required
	LedgerID                  string       // Also system correlation ID.
	LedgerStatus              LedgerStatus // Directional status towards terminus.
	LedgerCreatedAtEpochMilli int64        // CreatedAt for replayability

	// Optional
	TriggerEventPayload     string // article text, ...
	TriggerEventSource      string
	TriggerEventMediaUrls   string // Images, videos, ... [url1, url2,...]
	TriggerEventWebsiteUrls string // product pages, news article sources crawled. [url1, url2,...]
	TriggerEventLanguage    string // EN, CN, ... Specifies the overall downstream language as set by Drivers.
	TriggerEventContentHash string // for deduping raw events.
	MediaEvents             string // Media generation: audio, video, ...
	PublishEvents           string // Publish to distribution channel: YouTube, Instagram, ...
	MediaEventsVersion      int64
	PublishEventsVersion    int64
}
type Event interface {
	GetEventID() string
}

func (ledgerItem *Ledger) GetExistingMediaEvents() ([]MediaEvent, error) {
	var existingMediaEvents []MediaEvent
	if ledgerItem.MediaEvents == "" {
		return existingMediaEvents, nil
	}

	err := json.Unmarshal([]byte(ledgerItem.MediaEvents), &existingMediaEvents)
	if err != nil {
		log.Printf("error unmarshalling mediaEvents: %s", err)
		return existingMediaEvents, err
	}
	return existingMediaEvents, err
}

func (ledgerItem *Ledger) GetExistingPublishEvents() ([]PublishEvent, error) {
	var existingPublishEvents []PublishEvent
	if ledgerItem.PublishEvents == "" {
		return existingPublishEvents, nil
	}

	err := json.Unmarshal([]byte(ledgerItem.PublishEvents), &existingPublishEvents)
	if err != nil {
		log.Printf("error unmarshalling publishEvents: %s", err)
		return existingPublishEvents, err
	}
	return existingPublishEvents, err
}

// MediaType determine what downstream media-generator will be used for this MediaEvent.
type MediaType string

const (
	MEDIA_TEXT  MediaType = "Text"
	MEDIA_VIDEO MediaType = "Video"
	IMAGE       MediaType = "Image"
)

// DistributionFormat are only set for the Parent/Root MediaEvent.
// Used to select the applicable downstream PublisherProfile that supports the format.
// E.g. You cannot publish a Blog to Snapchat, but you can publish a Blog to Medium or Reddit.
type DistributionFormat string

const (
	DIST_FORMAT_BLOG   DistributionFormat = "Blog"
	DIST_FORMAT_LVIDEO DistributionFormat = "LongformVideo"
)

type PositionLayer string

const (
	FULLSCREEN       PositionLayer = "FULLSCREEN" // Occupies whole render space.
	SPLIT_SCR_TOP    PositionLayer = "SPLITSCREEN_TOP"
	SPLIT_SCR_BOTTOM PositionLayer = "SPLITSCREEN_BOTTOM"
	SPLIT_SCR_LEFT   PositionLayer = "SPLITSCREEN_LEFT"
	SPLIT_SCR_RIGHT  PositionLayer = "SPLITSCREEN_RIGHT"

	AVATAR         PositionLayer = "AVATAR"         // screen position for the talking head/body.
	AVATAR_OVERLAY PositionLayer = "AVATAR_OVERLAY" // apply user specified avatar as higher priority.
)

type MediaEvent struct {
	LedgerID                string             // Parent LedgerID
	PromptInstruction       string             // Instructions for the diffusion models. Will be used to vectorize & re-use media.
	SystemPromptInstruction string             // Roles, personalities, or response guidelines for the LLM.
	MediaType               MediaType          // Avatar, Avatar.Custom, Text, Video, ...; used to determine appropriate PGVector table.
	DistributionFormat      DistributionFormat // LongFormVideo, ShortFormVideo, Image, Blog, ...
	ContentLookupKey        string             // GUID into s3: e.g. <MediaType>.<SomeGuid>...
	Niche                   string             // Secondary filter on publish-profile results.
	Language                string             // Secondary filter on publish-profile results.
	PromptHash              string             // Hash of the prompt instruction
	EventID                 string             // Although derivable GetEventID, set for convenience on downstream calls.
	ParentEventID           string             // null for root. Will be set if part of a script ID.
	IsFinalRender           bool               // Used to indicate that this media will be uploaded to the target PublisherProfile distribution channel.
	VisualPositionLayer     string             // For determining position of video/image media in the final rendering.
	RenderSequence          int                // Determines order of media during final render. Multiple pieces of media can have same render sequence if concurrent.
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
	// <media_Type>.<ledgerId>.<guid>
	// LedgerId will be used to redrive ledgerItem from the s3 topic notifications
	return fmt.Sprintf("%s.%s.%s", m.MediaType, m.LedgerID, uuid.New().String())
}

func HashString(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

type PublishStatus string

const (
	ASSIGNED   PublishStatus = "ASSIGNED"   // Lock taken. Assumes base child-events are ready.
	OVERLAY    PublishStatus = "OVERLAY"    // Overlay specific user media such as custom avatars.
	RENDERING  PublishStatus = "RENDERING"  // Once all child-elements & watermarks are ready, combine to final edit.
	PUBLISHING PublishStatus = "PUBLISHING" // Once final edit is ready, publish.
	COMPLETE   PublishStatus = "COMPLETE"   // Terminal, success.
	EXPIRED    PublishStatus = "EXPIRED"    // Terminal, failure, timeout.
)

// Associating PublishEvent to a PublisherProfile. Used for softlocking.
type PublishEvent struct {
	LedgerID            string        // Parent LedgerID
	DistributionChannel string        // YouTube, Medium, Twitter, ...
	PublishStatus       PublishStatus // Soft lock: ASSIGNED, PUBLISHING, COMPLETE, EXPIRED.
	ExpiresAtTTL        int64         // Lifetime of assignment lock prior to entering EXPIRED state if no associated COMPLETE.
	PublisherProfileID  string
	OwnerAccountID      string // PublisherProfile owner.
	RootMediaEventID    string
	ProcessOwner        string // Agent guid performing the publish.
}

func (m *PublishEvent) GetEventID() string {
	// concat <script_Id>.<publisher_profile_id>.<publish_status>
	return fmt.Sprintf("%s.%s.%s", m.DistributionChannel, m.PublisherProfileID, m.PublishStatus)
}

func (m *PublishEvent) GetRootMediaAssignmentKey() string {
	// concat <script_Id>.<RootMediaEventID>.<publish_status>
	return fmt.Sprintf("%s.%s.%s", m.DistributionChannel, m.RootMediaEventID, m.PublishStatus)
}
