package manifest

import (
	"encoding/json"
	"log"
)

type BlogSchema struct {
	Instruction           string   `json:"instruction"`
	BlogTitle             string   `json:"blogTitle"`
	BlogText              string   `json:"blogText"`
	BlogHtml              string   `json:"blogHtml"`
	ImageDescriptionTexts []string `json:"imageDescriptionTexts"`
}

type TinyBlogSchema struct {
	Instruction           string   `json:"instruction"`
	BlogTitle             string   `json:"blogTitle"`
	BlogText              string   `json:"blogText"`
	ImageDescriptionTexts []string `json:"imageDescriptionTexts"`
}

type ShortVideoSchema struct {
	VideoTitle                string   `json:"videoTitle"` // json key should be consistent between Short and Long videos.
	VideoThumbnailText        string   `json:"videoThumbnailText"`
	VideoDescription          string   `json:"videoDescription"`
	VideoTags                 []string `json:"videoTags"`
	ThumbnailImageDescription string   `json:"thumbnailImageDescription"`
	MainPost                  string   `json:"mainPost"`
	Comments                  []string `json:"comments"`
}

func GetBlogJsonSchema() string {
	sampleShot := BlogSchema{
		Instruction: "The instructions you received must be in the instruction field.",
		BlogTitle:   "Your output blog title must be in the blogTitle field.",
		BlogText:    "Your output blog content as plaintext must be in the blogText field.",
		BlogHtml:    "Your output blog content as HTML must be in the blogHtml field.",
		ImageDescriptionTexts: []string{"At least one, and at most two descriptions of images that charachterize the blog text.",
			"One image description per entry in this json string array.",
			"Describe the images using excruciating details for calling an image generator.",
			`Include any details of the texture, lighting, text, objects, scenery,  placement arrangement, clothing, 
			skin color, tone, and anything else to accurately describe the image.`,
		},
	}

	b, err := json.MarshalIndent(sampleShot, "", "  ")
	if err != nil {
		log.Fatalf("error marshalling schema sample: %s", err)
	}
	return string(b)
}

func GetTinyBlogJson() string {
	sampleShot := TinyBlogSchema{
		Instruction: "The instructions you received must be in the instruction field.",
		BlogTitle:   "Your output blog title must be in the blogTitle field.",
		BlogText:    "Your output blog content as plaintext must be in the blogText field.",
		// TODO: Set this to at-least 0 https://trello.com/c/ie8Eh4n3
		ImageDescriptionTexts: []string{"Descriptions of images that charachterize the blog text go here.",
			"One image description per entry in this json string array.",
			"You are allowed 0, 1, or 2 description entries.", "The array may be empty.",
			"Describe the images using excruciating details for calling an image generator.",
			`Include any details of the texture, lighting, text, objects, scenery,  placement arrangement, clothing, 
			skin color, tone, and anything else to accurately describe the image.`,
		},
	}

	b, err := json.MarshalIndent(sampleShot, "", "  ")
	if err != nil {
		log.Fatalf("error marshalling schema sample: %s", err)
	}
	return string(b)
}

func GetShortVideoJson() string {
	sampleShot := ShortVideoSchema{
		VideoTitle: `Your clickbait video title goes here. Suffix your title with hashtag #shorts.
		Your title is pithy.
		Your title should evoke curiosity by asking a question, interest, and evoke strong emotions such as anger, fear, shock, surprise, or joy.`,
		VideoThumbnailText: `A highly condensed version of your videoTitle that is punchy and likely to solicit intrigue.
		Example:
		---Good Thumbnail Text 1---
		VideoTitle: My new wife wants to kick my daughter out of her room.
		VideoThumbnailText: Crazy Wife!
		Reasoning: The thumbnail text is short, punchy, captures the theme of the videoTitle.
		---Good Thumbnail Text 2---
		VideoTitle: The job market is the worst it's been since the 2008 crisis.
		VideoThumbnailText: NO HOPE LOSER
		Reasoning: The thumbnail text elicits intrigue by the phrase "no hope" and elicits an emotional response by adding "LOSER."
		`,
		VideoTags: []string{"Add search engine optimized keywords in json:videoTags array.", "You should generate at least 20 tags, and at most 30 tags.",
			"Your tags are two to three words each, with one tag per entry in json:videoTags array.", "Your tags bias toward long-tail keywords, specifics, and their synonyms.",
			"Total character length of the tags combined should be less than 400 characters."},
		VideoDescription: `Your video description should contain several hashtags, and an SEO rich description.
		You must include #shorts hashtag in the description.
		Include at least three relevant hashtags in your video description.`,
		ThumbnailImageDescription: `Describe an image likely to attract a viewer to click on your video, 
		and that is related to the videoTitle and videoDescription.`,
		MainPost: `Main post content, summary, or abridged text goes here.
		If the post is longer than one paragraph long, then abridge the contents to be less than one paragraph; summarizing to capture the main dramatic details.`,
		Comments: []string{
			"Comments from the post go here, summarized, or abridged.",
			"One comment per list entry.",
			"Select comments that are no more than two sentences long.",
		},
	}

	b, err := json.MarshalIndent(sampleShot, "", "  ")
	if err != nil {
		log.Fatalf("error marshalling schema sample: %s", err)
	}
	return string(b)
}
