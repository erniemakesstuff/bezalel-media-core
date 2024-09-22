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

type ScriptPromptCollection struct {
	ScriptPrompts []struct {
		PromptKey  string `yaml:"promptKey"`
		PromptText string `yaml:"promptText"`
	} `yaml:"scriptPrompts"`
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
	for _, p := range prompts.ScriptPrompts {
		log.Printf("PROMPTKEY: %s", p.PromptText)
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
	for _, p := range sources.Sources {
		log.Printf("SOURCE NAME: %s", p.SourceName)
	}
	return sources
}
