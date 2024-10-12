package manifest

import (
	"encoding/json"
	"log"
)

type BlogJsonSchema struct {
	Instruction string `json:"instruction"`
	BlogText    string `json:"blogText"`
}

func GetBlogJsonSchemaFewShot() string {
	sampleShot := BlogJsonSchema{
		Instruction: "instructions you received",
		BlogText:    "your blog content should go here",
	}

	b, err := json.MarshalIndent(sampleShot, "", "  ")
	if err != nil {
		log.Fatalf("error marshalling schema sample: %s", err)
	}
	return string(b)
}
