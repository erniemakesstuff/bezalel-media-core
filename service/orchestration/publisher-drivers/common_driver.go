package publisherdrivers

import (
	"encoding/json"
	"fmt"
	"log"

	manifest "github.com/bezalel-media-core/v2/manifest"
)

const BAD_REQUEST_PROFILE_CODE = "BadRequestProfileCode"

func ScriptPayloadToBlogJson(payload string) (manifest.BlogJsonSchema, error) {
	result := manifest.BlogJsonSchema{}
	err := json.Unmarshal([]byte(payload), &result)
	if err != nil {
		log.Printf("error unmarshalling script text to blog schema object: %s", err)
		log.Printf("error payload: <%s>", payload)
		return result, err
	}

	if len(result.BlogHtml) == 0 {
		// Try attempt substitute with "some" acceptable value.
		// Occurs when LLM refuses to populate blogHtml, but populates blogText.
		result.BlogHtml = result.BlogText
	}

	if len(result.BlogHtml) == 0 {
		return manifest.BlogJsonSchema{}, fmt.Errorf("medium empty payload received: %s", payload)
	}

	return result, err
}

func ScriptPayloadToTinyBlogJson(payload string) (manifest.TinyBlogJsonSchema, error) {
	result := manifest.TinyBlogJsonSchema{}
	err := json.Unmarshal([]byte(payload), &result)
	if err != nil {
		log.Printf("error unmarshalling script text to blog schema object: %s", err)
		log.Printf("error payload: <%s>", payload)
		return result, err
	}

	if len(result.BlogText) == 0 {
		return manifest.TinyBlogJsonSchema{}, fmt.Errorf("twitter empty payload received: %s", payload)
	}

	return result, err
}
