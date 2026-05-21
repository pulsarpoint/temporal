package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type Client struct {
	baseURL string
	model   string
	http    *http.Client
}

func NewClient(baseURL, model string) *Client {
	return &Client{baseURL: baseURL, model: model, http: &http.Client{}}
}

type chatRequest struct {
	Model    string    `json:"model"`
	Messages []message `json:"messages"`
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatResponse struct {
	Choices []struct {
		Message message `json:"message"`
	} `json:"choices"`
}

func (c *Client) TranslateMap(ctx context.Context, category string, inputs map[string]string) (map[string]string, error) {
	if len(inputs) == 0 {
		return map[string]string{}, nil
	}
	user, err := json.Marshal(inputs)
	if err != nil {
		return nil, fmt.Errorf("marshal translation inputs: %w", err)
	}
	content, err := c.complete(ctx,
		"You are a Norwegian-to-English business translator. Return a JSON object where each key is the original Norwegian text and each value is the accurate English translation. Return only the JSON object, no markdown, no explanation, no extra keys.",
		string(user),
	)
	if err != nil {
		return nil, fmt.Errorf("translate %s: %w", category, err)
	}
	var translated map[string]string
	if err := json.Unmarshal([]byte(content), &translated); err != nil {
		return nil, fmt.Errorf("parse translation JSON: %w", err)
	}
	for key := range translated {
		if _, ok := inputs[key]; !ok {
			return nil, fmt.Errorf("llm returned unexpected key %q", key)
		}
	}
	return translated, nil
}

func (c *Client) complete(ctx context.Context, system, user string) (string, error) {
	body, err := json.Marshal(chatRequest{
		Model: c.model,
		Messages: []message{
			{Role: "system", Content: system},
			{Role: "user", Content: user},
		},
	})
	if err != nil {
		return "", fmt.Errorf("marshal request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		errorBody, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("llm returned %d: %s", resp.StatusCode, string(errorBody))
	}
	var decoded chatResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}
	if len(decoded.Choices) == 0 {
		return "", fmt.Errorf("empty choices in response")
	}
	return decoded.Choices[0].Message.Content, nil
}
