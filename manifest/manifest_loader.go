package manifest

import (
	"log"
	"os"
	"strings"
	"sync"

	"gopkg.in/yaml.v2"
)

type ManifestLoader struct {
	ScriptPrompts                    ScriptPromptCollection
	SourceToScriptCategoryCollection SourceCollection
	DistributionFormatToChannel      DistributionFormatCollection
}

var manifestInstance *ManifestLoader
var once sync.Once

const (
	PROMPT_SCRIPT_VAR_RAW_TEXT    = "$RAW_TEXT"
	PROMPT_SCRIPT_VAR_LANGUAGE    = "$LANGUAGE"
	PROMPT_SCRIPT_VAR_BLOG_FORMAT = "$BLOG_JSON_FORMAT"
)

type Prompt struct {
	PromptCategoryKey string `yaml:"promptCategoryKey"` // Language.MediaType.Niche
	SystemPromptText  string `yaml:"systemPromptText"`
	PromptText        string `yaml:"promptText"`
}

func (p *Prompt) GetDistributionFormat() string {
	return strings.Split(p.PromptCategoryKey, ".")[0]
}

func (p *Prompt) GetNiche() string {
	return strings.Split(p.PromptCategoryKey, ".")[1]
}

type ScriptPromptCollection struct {
	ScriptPrompts []Prompt `yaml:"scriptPrompts"`
}

type SourceCollection struct {
	Sources []struct {
		SourceName       string `yaml:"sourceName"`
		ScriptCategories []struct {
			CategoryKey string `yaml:"categoryKey"`
		} `yaml:"scriptCategories"`
	} `yaml:"sources"`
}

type DistributionFormatCollection struct {
	DistributionFormats []struct {
		Format   string `yaml:"format"`
		Channels []struct {
			ChannelName string `yaml:"channelName"`
		} `yaml:"channels"`
	} `yaml:"distributionFormats"`
}

func GetManifestLoader() *ManifestLoader {
	if manifestInstance != nil {
		return manifestInstance
	}
	once.Do(func() {
		initManifest()
	})
	return manifestInstance
}

func (m *ManifestLoader) ChannelNamesFromFormat(mediaDistributionFormat string) []string {
	result := []string{}
	for _, f := range m.DistributionFormatToChannel.DistributionFormats {
		if strings.EqualFold(f.Format, mediaDistributionFormat) {
			for _, cn := range f.Channels {
				result = append(result, cn.ChannelName)
			}
		}
	}
	return result
}

func (m *ManifestLoader) GetScriptPromptsFromSource(sourceName string) []Prompt {
	categoryKeysFromSource := map[string]bool{}
	for _, source := range m.SourceToScriptCategoryCollection.Sources {
		if source.SourceName == sourceName {
			for _, category := range source.ScriptCategories {
				categoryKeysFromSource[category.CategoryKey] = true
			}
		}
	}

	resultPrompts := []Prompt{}
	for _, p := range m.ScriptPrompts.ScriptPrompts {
		if categoryKeysFromSource[p.PromptCategoryKey] {
			resultPrompts = append(resultPrompts, p)
		}
	}
	return resultPrompts
}

func initManifest() {
	manifest := ManifestLoader{
		ScriptPrompts:                    getScriptPromptCollection(),
		SourceToScriptCategoryCollection: getSourceToScriptCategoryCollection(),
		DistributionFormatToChannel:      getDistributionFormatToChannelCollection(),
	}
	manifestInstance = &manifest
}

func getScriptPromptCollection() ScriptPromptCollection {
	promptFile, err := os.ReadFile("./manifest/script_prompts.yml")
	if err != nil {
		log.Fatalf("failed to load file manifest prompts: %s", err)
	}

	var prompts ScriptPromptCollection
	err = yaml.Unmarshal(promptFile, &prompts)
	if err != nil {
		log.Fatalf("failed to unmarshall manifest prompts: %s", err)
	}

	for i := range prompts.ScriptPrompts {
		// TODO: chain other schema replacements here.
		prompts.ScriptPrompts[i].SystemPromptText = strings.Replace(prompts.ScriptPrompts[i].SystemPromptText,
			PROMPT_SCRIPT_VAR_BLOG_FORMAT, GetBlogJsonSchemaFewShot(), -1)
	}
	return prompts
}

func getSourceToScriptCategoryCollection() SourceCollection {
	promptFile, err := os.ReadFile("./manifest/source_to_script_categories.yml")
	if err != nil {
		log.Fatalf("failed to load file manifest sources: %s", err)
	}

	var sources SourceCollection
	err = yaml.Unmarshal(promptFile, &sources)
	if err != nil {
		log.Fatalf("failed to unmarshall manifest sources: %s", err)
	}
	return sources
}

func getDistributionFormatToChannelCollection() DistributionFormatCollection {
	distFile, err := os.ReadFile("./manifest/distribution_format_to_channel.yml")
	if err != nil {
		log.Fatalf("failed to load file manifest distribution format: %s", err)
	}

	var distFormats DistributionFormatCollection
	err = yaml.Unmarshal(distFile, &distFormats)
	if err != nil {
		log.Fatalf("failed to unmarshall manifest distribution format: %s", err)
	}
	return distFormats
}
