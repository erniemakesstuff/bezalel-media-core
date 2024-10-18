package manifest

import (
	"encoding/json"
	"log"
)

type BlogJsonSchema struct {
	Instruction string `json:"instruction"`
	BlogTitle   string `json:"blogTitle"`
	BlogText    string `json:"blogText"`
	BlogHtml    string `json:"blogHtml"`
}

type TinyBlogJsonSchema struct {
	Instruction string `json:"instruction"`
	BlogTitle   string `json:"blogTitle"`
	BlogText    string `json:"blogText"`
}

func GetBlogJsonSchemaFewShot() string {
	sampleShot := BlogJsonSchema{
		Instruction: "instructions you received",
		BlogTitle:   "your blog title goes here",
		BlogText:    "your blog content as plaintext",
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
		BlogText:    "your plaintext content goes here",
	}

	b, err := json.MarshalIndent(sampleShot, "", "  ")
	if err != nil {
		log.Fatalf("error marshalling schema sample: %s", err)
	}
	return string(b)
}
