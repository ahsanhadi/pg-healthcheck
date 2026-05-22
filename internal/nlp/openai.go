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

// openAIProvider calls the OpenAI chat completions API (or any OpenAI-compatible
// endpoint such as Azure OpenAI, Groq, Together AI, etc.).
type openAIProvider struct {
	baseURL string
	model   string
	apiKey  string
	timeout time.Duration
}

func (p *openAIProvider) Name() string { return "openai/" + p.model }

func (p *openAIProvider) Query(prompt string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()

	type message struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}
	type reqBody struct {
		Model     string    `json:"model"`
		Messages  []message `json:"messages"`
		MaxTokens int       `json:"max_tokens"`
	}
	body, err := json.Marshal(reqBody{
		Model:     p.model,
		Messages:  []message{{Role: "user", Content: prompt}},
		MaxTokens: 50,
	})
	if err != nil {
		return "", fmt.Errorf("openai: marshalling request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		p.baseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("openai: building request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+p.apiKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("openai: request failed: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("openai: reading response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("openai: HTTP %d: %s", resp.StatusCode, truncate(string(data), 200))
	}

	var result struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return "", fmt.Errorf("openai: parsing response: %w", err)
	}
	if len(result.Choices) == 0 {
		return "", fmt.Errorf("openai: empty choices in response")
	}
	return result.Choices[0].Message.Content, nil
}

// truncate returns at most n bytes of s (for error messages).
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
