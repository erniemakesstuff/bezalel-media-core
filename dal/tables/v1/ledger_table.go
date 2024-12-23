package v1

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/google/uuid"
)

type LedgerStatus string

const (
	NEW_LEDGER      LedgerStatus = "New"
	FINISHED_LEDGER LedgerStatus = "Finished" // Terminal for all cases: expired or success.
)

type Ledger struct {
	// Required
	LedgerID                  string       // Also system correlation ID.
	LedgerStatus              LedgerStatus // Directional status towards terminus.
	LedgerCreatedAtEpochMilli int64        // CreatedAt for replayability

	// Optional
	TriggerEventPayload        string // article text, ...
	TriggerEventSource         string
	TriggerEventMediaUrls      string // CSV Images, videos, ... [url1, url2,...]
	TriggerEventWebsiteUrls    string // CSV product pages, news article sources crawled. [url1, url2,...]
	TriggerEventTargetLanguage string // EN, CN, ... Specifies the overall downstream language as set by Drivers.
	TriggerEventContentHash    string // for deduping raw events.
	MediaEvents                string // Media generation: audio, video, ...
	PublishEvents              string // Publish to distribution channel: YouTube, Instagram, ...
	MediaEventsVersion         int64
	PublishEventsVersion       int64
	HeartbeatCount             int64
	TTL                        int64 // epoch seconds
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
	MEDIA_TEXT   MediaType = "Text"
	MEDIA_VIDEO  MediaType = "Video"
	MEDIA_IMAGE  MediaType = "Image"  // Png and jpeg. Ensure append suffix contentlookupkey when manipulating files.
	MEDIA_SFX    MediaType = "Sfx"    // Sound effects
	MEDIA_VOCAL  MediaType = "Vocal"  // Narration
	MEDIA_MUSIC  MediaType = "Music"  // Songs; other music.
	MEDIA_RENDER MediaType = "Render" // Multi-media; compilation; replacements. Thumbnail generation.
)

// DistributionFormat are only set for the Parent/Root MediaEvent.
// Used to select the applicable downstream PublisherProfile that supports the format.
// E.g. You cannot publish a Blog to Snapchat, but you can publish a Blog to Medium or Reddit.
type DistributionFormat string

const (
	DIST_FORMAT_INTEG_BLOG DistributionFormat = "IntegBlog"
	DIST_FORMAT_BLOG       DistributionFormat = "Blog"
	DIST_FORMAT_BLOG_TINY  DistributionFormat = "TinyBlog"
	DIST_FORMAT_LVIDEO     DistributionFormat = "LongVideo"
	DIST_FORMAT_SVIDEO     DistributionFormat = "ShortVideo"
)

type PositionLayer string

const (
	// For videos.
	FULLSCREEN       PositionLayer = "Fullscreen" // Occupies whole render space; also used for setting backgrounds.
	SPLIT_SCR_TOP    PositionLayer = "SplitScreenTop"
	SPLIT_SCR_BOTTOM PositionLayer = "SplitScreenBottom"
	SPLIT_SCR_LEFT   PositionLayer = "SplitScreenLeft"
	SPLIT_SCR_RIGHT  PositionLayer = "SplitScreenRight"
	SPLIT_SCR_CENTER PositionLayer = "SplitScreenCenter" // "cut out" in center of screen position.

	// Not specifying "where"; defering placement to render templates.
	AVATAR           PositionLayer = "Avatar"          // screen position for the talking head/body.
	AVATAR_OVERLAY   PositionLayer = "AvatarOverlay"   // apply user specified avatar as higher priority.
	AVATAR_THUMBNAIL PositionLayer = "AvatarThumbnail" // used for "shock" expressive facial image overlaid on thumbnail image.

	// For static / text final media.
	IMAGE_TOP        PositionLayer = "ImageOnTop"
	IMAGE_BOTTOM     PositionLayer = "ImageOnBottom"
	IMAGE_CENTER     PositionLayer = "ImageCenter"
	IMAGE_ATTACHMENT PositionLayer = "ImageAttachment" // Attach wherever.
	IMAGE_THUMBNAIL  PositionLayer = "Thumbnail"       // For video final renders; fullscreen.

	// Audio
	BACKGROUND_MUSIC PositionLayer = "BackgroundMusic"
	NARRATOR         PositionLayer = "Narrator"
	SOUND            PositionLayer = "Sound" // Catch-all for other background audio such as sfx.
	// Hidden; other metadata
	HIDDEN PositionLayer = "Hidden"
	SCRIPT PositionLayer = "HiddenScript"
)

type RenderMediaSequence struct {
	EventID          string
	MediaType        MediaType
	PositionLayer    PositionLayer
	ContentLookupKey string
	// Grouped by PositionLayer.
	// RenderSequences are strictly increasing within a PositionLayer
	// RenderSequences may collid across PositionLayers, this alows for overlaying sounds, effects, of imagery.
	// E.g.
	// Narrator layer: 0, 1, 2 ==> narration sounds should play sequentially in the defined order.
	// BackgroundMusic: 1 ==> background music SHOULD START at 1.
	// When compositing, at the second narration sound, the background music plays concurrently.
	//
	// RenderSequences only determine sequencing only within their audio or visual layers.
	// However, background music is treated as it's own "sub-layer" within audio layer.
	// This is to allow background music to play in the background.
	// VisualLayer, Fullscreen: 0, 1, ..., 4
	// Sfx: 5
	// BackgroundMusic: 4
	// Background music will play at the beginning of the video, despite having sequence number 4
	// Sfx will play at the beggining because background music is treated as a sub-layer.
	//
	// Image: 1
	// Video: 1
	// Both image and video will play concurrently.
	//
	// A prescriptive model that trades flexibility for generally desireable outcomes: minimizing "dead air"
	// in a hyper-active, hyper-attentive media landscape.
	RenderSequence int
}

type MetaMediaDescriptor string

const (
	FINAL_RENDER    MetaMediaDescriptor = "FinalRender"       // Used to indicate that this media will be uploaded to the target PublisherProfile distribution channel.
	SCRIPT_ENRICHED MetaMediaDescriptor = "ScriptWasEnriched" // metadata to indicate script data was enriched.
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

	// Set on enrichment parsing JSON callback from script process. Script prompt drives template json.
	PositionLayer PositionLayer // For determining position of video/image media in the final rendering.
	// Determines order of media during final render. Multiple pieces of media can have same render sequence if concurrent [0, N]
	RenderSequence int

	// Set on final rendering.
	FinalRenderSequences string // json. []RenderMediaSequence
	WatermarkText        string

	// Metadata
	RestrictToPublisherID string // publisher ID owning this render media; prevents re-assignment.
	MetaMediaDescriptor   MetaMediaDescriptor
}

func GetDistributionFormatFromString(format string) (DistributionFormat, error) {
	switch {
	case strings.EqualFold(format, string(DIST_FORMAT_INTEG_BLOG)):
		return DIST_FORMAT_INTEG_BLOG, nil
	case strings.EqualFold(format, string(DIST_FORMAT_BLOG)):
		return DIST_FORMAT_BLOG, nil
	case strings.EqualFold(format, string(DIST_FORMAT_BLOG_TINY)):
		return DIST_FORMAT_BLOG_TINY, nil
	case strings.EqualFold(format, string(DIST_FORMAT_LVIDEO)):
		return DIST_FORMAT_LVIDEO, nil
	case strings.EqualFold(format, string(DIST_FORMAT_SVIDEO)):
		return DIST_FORMAT_SVIDEO, nil
	}

	return DIST_FORMAT_BLOG, fmt.Errorf("unable to find matching distribution format from string: %s", format)
}

func (m *MediaEvent) SetEventID() {
	// derivable concatenation <Language>.<MediaType>.<Niche>.<PromptInstructionHash>: E.g. EN.LongFormVideo.NewsReport....
	// Enforce idempotency within the context of a ledger entry; no datastore collision.
	m.EventID = fmt.Sprintf("%s.%s.%s.%s", m.Language, m.MediaType, m.Niche, m.PromptHash)
}

func (m *MediaEvent) SetContentLookupKey() {
	// Use guid because promptHash for static-scripts will collide.
	// <media_Type>.<ledgerId>.<guid>.<media_file_extention>
	// LedgerId will be used to redrive ledgerItem from the s3 topic notifications
	m.ContentLookupKey = fmt.Sprintf("%s.%s.%s.%s", m.MediaType, m.LedgerID, uuid.New().String(), m.getFileExtension())
}

func (m *MediaEvent) getFileExtension() string {
	switch {
	case MEDIA_IMAGE == m.MediaType:
		return "png"
	case MEDIA_TEXT == m.MediaType:
		return "json"
	case MEDIA_RENDER == m.MediaType:
		return "render" // not a real file extension; metadata. Should resolve either .json, .mp4, .mp3 etc by the Publisher.
	case MEDIA_VIDEO == m.MediaType:
		return "mp4"
	case MEDIA_MUSIC == m.MediaType || MEDIA_SFX == m.MediaType || MEDIA_VOCAL == m.MediaType:
		return "mp3"
	}

	log.Fatal("no matching file extension for media type: " + string(m.MediaType))
	return "ERR"
}

func (m *MediaEvent) ToRenderSequence() RenderMediaSequence {
	return RenderMediaSequence{
		EventID:          m.EventID,
		MediaType:        m.MediaType, // Should not be Render-type! >:(
		PositionLayer:    m.PositionLayer,
		RenderSequence:   m.RenderSequence,
		ContentLookupKey: m.ContentLookupKey,
	}
}

func (m *MediaEvent) GetRenderSequences() ([]RenderMediaSequence, error) {
	var sequences []RenderMediaSequence
	if m.FinalRenderSequences == "" {
		return sequences, nil
	}

	err := json.Unmarshal([]byte(m.FinalRenderSequences), &sequences)
	if err != nil {
		log.Printf("error unmarshalling render sequences: %s", err)
		return sequences, err
	}
	return sequences, err
}

func (m *MediaEvent) ToMetadataEventEntry(metaDescriptor MetaMediaDescriptor,
	pubProfileId string, desiredMediaType MediaType) MediaEvent {
	copy := *m
	result := copy
	result.MetaMediaDescriptor = metaDescriptor
	result.PromptInstruction = fmt.Sprintf("OriginalPromptHash: %s - MetaDescriptor: %s - OPT_PUB: %s", m.PromptHash, string(metaDescriptor), pubProfileId)
	result.SystemPromptInstruction = fmt.Sprintf("OriginalPromptHash: %s - MetaDescriptor: %s - OPT_PUB: %s", m.PromptHash, string(metaDescriptor), pubProfileId)
	result.RestrictToPublisherID = pubProfileId
	result.MediaType = desiredMediaType
	result.ParentEventID = m.EventID
	result.PromptHash = HashString(result.PromptInstruction)
	result.SetEventID()
	result.SetContentLookupKey()
	return result
}

func (m *MediaEvent) ToChildMediaEntry(promptText string, promptSystemInstruction string, desiredMediaType MediaType) MediaEvent {
	copy := *m
	result := copy
	result.PromptInstruction = promptText
	result.SystemPromptInstruction = promptSystemInstruction
	result.MediaType = desiredMediaType
	result.ParentEventID = m.EventID
	result.PromptHash = HashString(result.PromptInstruction)
	result.SetEventID()
	result.SetContentLookupKey()
	return result
}

// Don't publish metadata entries that are used solely core core-service instruction.
// Nothing to render; generate.
func (m *MediaEvent) IsMetaPurposeOnly() bool {
	return m.MetaMediaDescriptor == SCRIPT_ENRICHED
}

func HashString(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

type ByRenderSequence []MediaEvent

func (a ByRenderSequence) Len() int           { return len(a) }
func (a ByRenderSequence) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByRenderSequence) Less(i, j int) bool { return a[i].RenderSequence < a[j].RenderSequence }

type PublishStatus string

const (
	ASSIGNED   PublishStatus = "Assigned"   // Lock taken. Assumes base child-events are ready.
	OVERLAY    PublishStatus = "Overlay"    // Overlay specific user media such as custom avatars.
	RENDERING  PublishStatus = "Rendering"  // Once all child-elements & watermarks are ready, combine to final edit.
	PUBLISHING PublishStatus = "Publishing" // Once final edit is ready, publish.
	COMPLETE   PublishStatus = "Complete"   // Terminal, success.
	EXPIRED    PublishStatus = "Expired"    // Terminal, failure, timeout.
)

// Associating PublishEvent to a PublisherProfile. Used for softlocking.
type PublishEvent struct {
	LedgerID            string        // Parent LedgerID
	DistributionChannel string        // YouTube, Medium, Twitter, ...
	PublishStatus       PublishStatus // Soft lock: ASSIGNED, PUBLISHING, COMPLETE, EXPIRED.
	ExpiresAtTTL        int64         // Lifetime of assignment lock prior to entering EXPIRED state if no associated COMPLETE.
	PublisherProfileID  string
	AccountID           string // PublisherProfile owner.
	RootMediaEventID    string
	ProcessOwner        string // Agent guid performing the publish.

	ChannelContentIDsCsv string // The content IDs; csv; from the downstream service YouTube, Twitter, etc.
}

func (m *PublishEvent) GetEventID() string {
	// concat <distId>.<account_id>.<publisher_profile_id>.<publish_status>
	return fmt.Sprintf("%s.%s.%s.%s", m.DistributionChannel, m.AccountID, m.PublisherProfileID, m.PublishStatus)
}

func (m *PublishEvent) GetEventIDByState(state PublishStatus) string {
	// concat <distId>.<account_id>.<publisher_profile_id>.<publish_status>
	return fmt.Sprintf("%s.%s.%s.%s", m.DistributionChannel, m.AccountID, m.PublisherProfileID, state)
}

func (m *PublishEvent) GetRootMediaAssignmentKey() string {
	// concat <distId>.<RootMediaEventID>.<publish_status>
	return fmt.Sprintf("%s.%s.%s", m.DistributionChannel, m.RootMediaEventID, m.PublishStatus)
}

func (m *PublishEvent) GetRootMediaAssignmentKeyByState(state PublishStatus) string {
	// concat <distId>.<RootMediaEventID>.<publish_status>
	return fmt.Sprintf("%s.%s.%s", m.DistributionChannel, m.RootMediaEventID, state)
}

func RootMediaKey(channel string, mediaEventId string, state PublishStatus) string {
	// concat <distId>.<RootMediaEventID>.<publish_status>
	return fmt.Sprintf("%s.%s.%s", channel, mediaEventId, state)
}
