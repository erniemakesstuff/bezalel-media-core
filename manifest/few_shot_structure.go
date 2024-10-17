package manifest

import (
	"encoding/json"
	"log"
)

type BlogJsonSchema struct {
	Instruction string `json:"instruction"`
	BlogTitle   string `json:"blogTitle"`
	BlogHtml    string `json:"blogHtml"`
}

type TinyBlogJsonSchema struct {
	Instruction string `json:"instruction"`
	BlogTitle   string `json:"blogTitle"`
	BlogText    string `json:"blogText"`
	BlogHtml    string `json:"blogHtml"`
}

func GetBlogJsonSchemaFewShot() string {
	sampleShot := BlogJsonSchema{
		Instruction: "instructions you received",
		BlogTitle:   "your blog title goes here",
		BlogHtml:    "your blog content as HTML",
	}

	b, err := json.MarshalIndent(sampleShot, "", "  ")
	if err != nil {
		log.Fatalf("error marshalling schema sample: %s", err)
	}
	return string(b)
}

func GetTinyBlogJson() string {
	sampleShot := TinyBlogJsonSchema{
		Instruction: "instructions you received",
		BlogTitle:   "your blog title goes here",
		BlogText:    "your raw blog text content goes here",
	}

	b, err := json.MarshalIndent(sampleShot, "", "  ")
	if err != nil {
		log.Fatalf("error marshalling schema sample: %s", err)
	}
	return string(b)
}
