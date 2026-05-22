package nlp

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

const geminiBaseURL = "https://generativelanguage.googleapis.com/v1beta/models"

// geminiProvider calls the Google Gemini generateContent API.
type geminiProvider struct {
	model   string
	apiKey  string
	timeout time.Duration
}

func (p *geminiProvider) Name() string { return "gemini/" + p.model }

func (p *geminiProvider) Query(prompt string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()

	body, err := json.Marshal(geminiRequest(prompt))
	if err != nil {
		return "", fmt.Errorf("gemini: marshalling request: %w", err)
	}

	url := fmt.Sprintf("%s/%s:generateContent?key=%s", geminiBaseURL, p.model, p.apiKey)
	data, err := doJSONPost(ctx, url, body, nil)
	if err != nil {
		return "", fmt.Errorf("gemini: %w", err)
	}
	return parseGeminiResponse(data)
}

func geminiRequest(prompt string) any {
	type part struct {
		Text string `json:"text"`
	}
	type content struct {
		Parts []part `json:"parts"`
	}
	type body struct {
		Contents []content `json:"contents"`
	}
	return body{Contents: []content{{Parts: []part{{Text: prompt}}}}}
}

func parseGeminiResponse(data []byte) (string, error) {
	var result struct {
		Candidates []struct {
			Content struct {
				Parts []struct {
					Text string `json:"text"`
				} `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return "", fmt.Errorf("gemini: parsing response: %w", err)
	}
	if len(result.Candidates) == 0 || len(result.Candidates[0].Content.Parts) == 0 {
		return "", fmt.Errorf("gemini: empty candidates in response")
	}
	return result.Candidates[0].Content.Parts[0].Text, nil
}
