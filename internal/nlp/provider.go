package nlp

import (
	"fmt"
	"os"
	"time"

	"github.com/pgedge/pg-healthcheck/internal/config"
)

// Provider is the interface implemented by every LLM backend.
// Ask sends a prompt and returns the model's raw text response.
type Provider interface {
	Ask(prompt string) (string, error)
	// Name returns a human-readable label such as "ollama/llama3.2".
	Name() string
}

// defaultModels holds sensible model names for cloud providers when the user
// has not explicitly overridden OllamaModel in the config.
var defaultModels = map[string]string{
	"openai": "gpt-4o-mini",
	"gemini": "gemini-2.0-flash",
}

// NewProvider constructs the Provider selected by cfg.LLMProvider.
//
// Provider selection:
//   - "ollama"  (default) — local Ollama server; no API key needed
//   - "openai"            — OpenAI chat completions; also compatible with
//     Azure OpenAI, Groq, etc. via a custom base URL
//   - "gemini"            — Google Gemini generateContent API
//
// API key resolution order for cloud providers: CLI flag (cfg.LLMAPIKey) →
// YAML field → provider-specific env var (OPENAI_API_KEY / GEMINI_API_KEY).
// Returns an error if a cloud provider is selected but no key is found;
// MapQuery will fall back to keyword matching in that case.
func NewProvider(cfg *config.Config) (Provider, error) {
	timeout := time.Duration(cfg.OllamaTimeoutSeconds) * time.Second
	model := cfg.OllamaModel

	switch cfg.LLMProvider {
	case "openai":
		return buildOpenAIProvider(cfg, timeout, model)
	case "gemini":
		return buildGeminiProvider(cfg, timeout, model)
	default: // "ollama" or anything unrecognised
		if model == "" {
			model = "llama3.2"
		}
		return &ollamaProvider{host: cfg.OllamaHost, model: model, timeout: timeout}, nil
	}
}

func buildOpenAIProvider(cfg *config.Config, timeout time.Duration, model string) (Provider, error) {
	apiKey := resolveAPIKey(cfg.LLMAPIKey, "OPENAI_API_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("provider openai requires an API key; set --api-key, llm_api_key in config, or OPENAI_API_KEY env var")
	}
	if model == "" || model == "llama3.2" {
		model = defaultModels["openai"]
	}
	baseURL := cfg.OllamaHost
	if baseURL == "" || baseURL == "http://localhost:11434" {
		baseURL = "https://api.openai.com"
	}
	return &openAIProvider{baseURL: baseURL, model: model, apiKey: apiKey, timeout: timeout}, nil
}

func buildGeminiProvider(cfg *config.Config, timeout time.Duration, model string) (Provider, error) {
	apiKey := resolveAPIKey(cfg.LLMAPIKey, "GEMINI_API_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("provider gemini requires an API key; set --api-key, llm_api_key in config, or GEMINI_API_KEY env var")
	}
	if model == "" || model == "llama3.2" {
		model = defaultModels["gemini"]
	}
	return &geminiProvider{model: model, apiKey: apiKey, timeout: timeout}, nil
}

// resolveAPIKey returns the first non-empty value from: explicit key, env var.
func resolveAPIKey(explicit, envVar string) string {
	if explicit != "" {
		return explicit
	}
	return os.Getenv(envVar)
}
