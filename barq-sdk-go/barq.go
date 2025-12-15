package barq

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type Config struct {
	BaseURL string
	APIKey  string
}

type Client struct {
	config Config
	http   *http.Client
}

func NewClient(config Config) *Client {
	return &Client{
		config: config,
		http: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *Client) request(ctx context.Context, method, path string, body interface{}) ([]byte, error) {
	url := fmt.Sprintf("%s%s", strings.TrimRight(c.config.BaseURL, "/"), path)
	
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		bodyReader = bytes.NewBuffer(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", c.config.APIKey)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("api error %d: %s", resp.StatusCode, string(respBytes))
	}

	return respBytes, nil
}

type CreateCollectionRequest struct {
	Name       string      `json:"name"`
	Dimension  int         `json:"dimension"`
	Metric     string      `json:"metric"`
	Index      interface{} `json:"index,omitempty"`
	TextFields []TextField `json:"text_fields,omitempty"`
}

type TextField struct {
	Name     string `json:"name"`
	Indexed  bool   `json:"indexed"`
	Required bool   `json:"required"`
}

func (c *Client) CreateCollection(ctx context.Context, req CreateCollectionRequest) error {
	_, err := c.request(ctx, "POST", "/collections", req)
	return err
}

type InsertRequest struct {
	ID      interface{}     `json:"id"`
	Vector  []float32       `json:"vector"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

func (c *Client) Insert(ctx context.Context, collection string, req InsertRequest) error {
	path := fmt.Sprintf("/collections/%s/documents", collection)
	_, err := c.request(ctx, "POST", path, req)
	return err
}

type SearchRequest struct {
	Vector []float32   `json:"vector,omitempty"`
	Query  string      `json:"query,omitempty"`
	TopK   int         `json:"top_k"`
	Filter interface{} `json:"filter,omitempty"`
}

type SearchResponse struct {
	Results []SearchResult `json:"results"`
}

type SearchResult struct {
	ID    interface{} `json:"id"`
	Score float32     `json:"score"`
}

func (c *Client) Search(ctx context.Context, collection string, req SearchRequest) ([]SearchResult, error) {
	path := fmt.Sprintf("/collections/%s/search", collection)
	if req.Vector != nil && req.Query != "" {
		path += "/hybrid"
	} else if req.Query != "" {
		path += "/text"
	}

	respBytes, err := c.request(ctx, "POST", path, req)
	if err != nil {
		return nil, err
	}

	var resp SearchResponse
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		return nil, err
	}
	return resp.Results, nil
}
