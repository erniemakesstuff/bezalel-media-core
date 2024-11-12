package manifest

import (
	"encoding/json"
	"log"
)

type BlogJsonSchema struct {
	Instruction           string   `json:"instruction"`
	BlogTitle             string   `json:"blogTitle"`
	BlogText              string   `json:"blogText"`
	BlogHtml              string   `json:"blogHtml"`
	ImageDescriptionTexts []string `json:"imageDescriptionTexts"`
}

type TinyBlogJsonSchema struct {
	Instruction           string   `json:"instruction"`
	BlogTitle             string   `json:"blogTitle"`
	BlogText              string   `json:"blogText"`
	ImageDescriptionTexts []string `json:"imageDescriptionTexts"`
}

func GetBlogJsonSchema() string {
	sampleShot := BlogJsonSchema{
		Instruction: "The instructions you received must be in the instruction field.",
		BlogTitle:   "Your output blog title must be in the blogTitle field.",
		BlogText:    "Your output blog content as plaintext must be in the blogText field.",
		BlogHtml:    "Your output blog content as HTML must be in the blogHtml field.",
		ImageDescriptionTexts: []string{"At least one, and at most two descriptions of images that charachterize the blog text.",
			"One image description per entry in this json string array."},
	}

	b, err := json.MarshalIndent(sampleShot, "", "  ")
	if err != nil {
		log.Fatalf("error marshalling schema sample: %s", err)
	}
	return string(b)
}

func GetTinyBlogJson() string {
	sampleShot := TinyBlogJsonSchema{
		Instruction: "The instructions you received must be in the instruction field.",
		BlogTitle:   "Your output blog title must be in the blogTitle field.",
		BlogText:    "Your output blog content as plaintext must be in the blogText field.",
		// TODO: Set this to at-least 0 https://trello.com/c/ie8Eh4n3
		ImageDescriptionTexts: []string{"At least one, and at most two descriptions of images that charachterize the blog text.",
			"One image description per entry in this json string array."},
	}

	b, err := json.MarshalIndent(sampleShot, "", "  ")
	if err != nil {
		log.Fatalf("error marshalling schema sample: %s", err)
	}
	return string(b)
}
