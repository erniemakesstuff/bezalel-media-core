package orchestration

import (
	"fmt"
	"math/rand"

	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
	"github.com/bezalel-media-core/v2/manifest"
)

func createBlogChildEventsFromImageDescriptions(imageDescriptions []string, parentMediaEvent tables.MediaEvent,
	existingMediaEvents []tables.MediaEvent) []tables.MediaEvent {
	idMap := CreateMediaMapByEventId(existingMediaEvents)
	events := []tables.MediaEvent{}
	const systemInstruction = "Generate an image from the text prompt."
	for idx, imgD := range imageDescriptions {
		e := parentMediaEvent.ToChildMediaEntry(imgD, systemInstruction, tables.MEDIA_IMAGE)
		e.RenderSequence = idx
		e.PositionLayer = tables.IMAGE_ATTACHMENT
		_, ok := idMap[e.EventID]
		if !ok {
			events = append(events, e)
		}
	}
	return events
}

func createShortVideoChildEvents(schema manifest.ShortVideoSchema, parentMediaEvent tables.MediaEvent,
	existingMediaEvents []tables.MediaEvent) []tables.MediaEvent {
	events := []tables.MediaEvent{}
	idMap := CreateMediaMapByEventId(existingMediaEvents)
	// Thumbnail
	// Thumbnail instruction isn't used while we're using lexica. However, will be used when we start generatig our own images in-house.
	thumbnailInstruct := `Generate a video thumbnail image according to the given prompt.
		Add the following text to the image using vibrant colors likely to attract a viewers attention: ` + schema.VideoTitle
	thumbnail := parentMediaEvent.ToChildMediaEntry(schema.ThumbnailImageDescription, thumbnailInstruct, tables.MEDIA_IMAGE)
	thumbnail.RenderSequence = 0
	thumbnail.PositionLayer = tables.IMAGE_THUMBNAIL
	_, ok := idMap[thumbnail.EventID]
	if !ok {
		events = append(events, thumbnail)
	}

	// Static brainrot videos; append 5 rand. Will be cut and trimmed in final rendering.
	const staticPrompt = "Static content; not used in generation."
	const maxBrainrotBackgroundVideo = 5
	for i := 1; i <= maxBrainrotBackgroundVideo; i++ {
		vidBg := parentMediaEvent.ToChildMediaEntry(staticPrompt, staticPrompt, tables.MEDIA_VIDEO)
		vidBg.RenderSequence = i
		vidBg.PositionLayer = tables.FULLSCREEN
		// Upload to more to s3, then update here :)
		const maxStaticBrainrotBackground = 44 // exclusive
		randIntVal := rand.Intn(maxStaticBrainrotBackground)
		vidBg.ContentLookupKey = fmt.Sprintf("b%d.mp4", randIntVal)
		_, ok := idMap[vidBg.EventID]
		if !ok {
			events = append(events, vidBg)
		}
	}

	// Background music
	musicBg := parentMediaEvent.ToChildMediaEntry(staticPrompt, staticPrompt, tables.MEDIA_MUSIC)
	musicBg.RenderSequence = 0 // RenderSequences are grouped by their position layer in the final edit.
	musicBg.PositionLayer = tables.BACKGROUND_MUSIC
	// Upload to more to s3, then update here :)
	const maxStaticMusic = 8 // exclusive
	randIntVal := rand.Intn(maxStaticMusic)
	musicBg.ContentLookupKey = fmt.Sprintf("m%d.mp3", randIntVal)
	_, ok = idMap[musicBg.EventID]
	if !ok {
		events = append(events, musicBg)
	}

	// Narration
	const narrationPrompt = "Read the text in a male voice."
	narrationContent := []string{schema.MainPost}
	narrationContent = append(narrationContent, schema.Comments...)
	for i := 0; i < len(narrationContent); i++ {
		narrator := parentMediaEvent.ToChildMediaEntry(narrationContent[i], narrationPrompt, tables.MEDIA_VOCAL)
		narrator.RenderSequence = i
		narrator.PositionLayer = tables.NARRATOR
		_, ok = idMap[narrator.EventID]
		if !ok {
			events = append(events, narrator)
		}
	}
	return events
}
