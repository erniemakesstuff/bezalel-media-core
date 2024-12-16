package publisherdrivers

import (
	"encoding/json"
	"fmt"
	"log"

	manifest "github.com/bezalel-media-core/v2/manifest"
)

const BAD_REQUEST_PROFILE_CODE = "BadRequestProfileCode"

func ScriptPayloadToBlogSchema(payload string) (manifest.BlogSchema, error) {
	result := manifest.BlogSchema{}
	err := json.Unmarshal([]byte(payload), &result)
	if err != nil {
		log.Printf("error payload <%s> unmarshalling script text to blog schema object: %s", payload, err)
		return result, err
	}

	if len(result.BlogHtml) == 0 {
		// Try attempt substitute with "some" acceptable value.
		// Occurs when LLM refuses to populate blogHtml, but populates blogText.
		result.BlogHtml = result.BlogText
	}

	if len(result.BlogHtml) == 0 {
		return manifest.BlogSchema{}, fmt.Errorf("empty blogHtml payload received: %s", payload)
	}

	return result, err
}

func ScriptPayloadToTinyBlogSchema(payload string) (manifest.TinyBlogSchema, error) {
	result := manifest.TinyBlogSchema{}
	err := json.Unmarshal([]byte(payload), &result)
	if err != nil {
		log.Printf("error unmarshalling script text to blog schema object: %s", err)
		log.Printf("error payload: <%s>", payload)
		return result, err
	}

	if len(result.BlogText) == 0 {
		return manifest.TinyBlogSchema{}, fmt.Errorf("empty blog text payload received: %s", payload)
	}

	return result, err
}

func ScriptPayloadToShortVideoSchema(payload string) (manifest.ShortVideoSchema, error) {
	result := manifest.ShortVideoSchema{}
	err := json.Unmarshal([]byte(payload), &result)
	if err != nil {
		log.Printf("error unmarshalling script text to blog schema object: %s", err)
		log.Printf("error payload: <%s>", payload)
		return result, err
	}

	if len(result.VideoTitle) == 0 {
		return manifest.ShortVideoSchema{}, fmt.Errorf("empty video title received: %s", payload)
	}

	return result, err
}
