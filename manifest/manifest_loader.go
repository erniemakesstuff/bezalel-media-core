package manifest

import (
	"log"
	"os"
	"sync"

	"gopkg.in/yaml.v2"
)

type ManifestLoader struct {
	ScriptPrompts                    ScriptPromptCollection
	SourceToScriptCategoryCollection SourceCollection
}

var manifestInstance *ManifestLoader
var once sync.Once

type Prompt struct {
	PromptCategoryKey string `yaml:"promptCategoryKey"`
	SystemPromptText  string `yaml:"systemPromptText"`
	PromptText        string `yaml:"promptText"`
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

func GetManifestLoader() *ManifestLoader {
	if manifestInstance != nil {
		return manifestInstance
	}
	once.Do(func() {
		initManifest()
	})
	return manifestInstance
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
