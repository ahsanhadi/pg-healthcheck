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

	type part struct {
		Text string `json:"text"`
	}
	type content struct {
		Parts []part `json:"parts"`
	}
	type reqBody struct {
		Contents []content `json:"contents"`
	}
	body, err := json.Marshal(reqBody{
		Contents: []content{{Parts: []part{{Text: prompt}}}},
	})
	if err != nil {
		return "", fmt.Errorf("gemini: marshalling request: %w", err)
	}

	url := fmt.Sprintf("%s/%s:generateContent?key=%s", geminiBaseURL, p.model, p.apiKey)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("gemini: building request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("gemini: request failed: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("gemini: reading response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("gemini: HTTP %d: %s", resp.StatusCode, truncate(string(data), 200))
	}

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
