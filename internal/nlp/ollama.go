package nlp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// ── ollamaProvider — implements Provider ────────────────────────────────────

type ollamaProvider struct {
	host    string
	model   string
	timeout time.Duration
}

func (p *ollamaProvider) Ask(prompt string) (string, error) {
	return QueryOllama(p.host, p.model, prompt, p.timeout)
}

func (p *ollamaProvider) Name() string { return "ollama/" + p.model }

// ── HTTP client ─────────────────────────────────────────────────────────────

type ollamaRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
	Stream bool   `json:"stream"`
}

type ollamaResponse struct {
	Response string `json:"response"`
}

// QueryOllama sends a prompt to the Ollama API and returns the response text.
// Returns an error if the server is unreachable or returns a non-200 status.
func QueryOllama(host, model, prompt string, timeout time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	body, err := json.Marshal(ollamaRequest{
		Model:  model,
		Prompt: prompt,
		Stream: false,
	})
	if err != nil {
		return "", fmt.Errorf("marshalling request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		host+"/api/generate", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("building request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("ollama unreachable: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("ollama returned HTTP %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading ollama response: %w", err)
	}

	var result ollamaResponse
	if err := json.Unmarshal(data, &result); err != nil {
		return "", fmt.Errorf("parsing ollama response: %w", err)
	}

	return result.Response, nil
}
