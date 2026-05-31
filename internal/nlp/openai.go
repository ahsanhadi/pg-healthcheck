package nlp

import (
	"context"
	"encoding/json"
	"fmt"
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

func (p *openAIProvider) Ask(prompt string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()

	body, err := json.Marshal(openAIRequest(p.model, prompt))
	if err != nil {
		return "", fmt.Errorf("openai: marshalling request: %w", err)
	}

	headers := map[string]string{"Authorization": "Bearer " + p.apiKey}
	data, err := doJSONPost(ctx, p.baseURL+"/v1/chat/completions", body, headers)
	if err != nil {
		return "", fmt.Errorf("openai: %w", err)
	}
	return parseOpenAIResponse(data)
}

func openAIRequest(model, prompt string) any {
	type message struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}
	type body struct {
		Model     string    `json:"model"`
		Messages  []message `json:"messages"`
		MaxTokens int       `json:"max_tokens"`
	}
	return body{
		Model:     model,
		Messages:  []message{{Role: "user", Content: prompt}},
		MaxTokens: 50,
	}
}

func parseOpenAIResponse(data []byte) (string, error) {
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
