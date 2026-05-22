package nlp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
)

// doJSONPost sends a JSON body to url, setting Content-Type and any extra
// headers, then returns the raw response bytes. It returns an error for
// network failures and non-200 status codes (with up to 200 bytes of the
// error body appended for diagnostics).
func doJSONPost(ctx context.Context, url string, body []byte, headers map[string]string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body)) //nolint:gosec // nosemgrep -- url is always a provider-controlled endpoint, never user input
	if err != nil {
		return nil, fmt.Errorf("building request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncate(string(data), 200))
	}
	return data, nil
}
