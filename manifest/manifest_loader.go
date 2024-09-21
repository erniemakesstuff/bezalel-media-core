package manifest

import (
	"log"
	"os"
	"sync"

	"gopkg.in/yaml.v2"
)

type ManifestLoader struct {
	ScriptPrompts ScriptPromptCollection
}

var manifestInstance *ManifestLoader
var once sync.Once

type ScriptPromptCollection struct {
	ScriptPrompts []struct {
		PromptKey  string `yaml:"promptKey"`
		PromptText string `yaml:"promptText"`
	} `yaml:"scriptPrompts"`
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
	manifest := ManifestLoader{
		ScriptPrompts: prompts,
	}
	manifestInstance = &manifest
}
