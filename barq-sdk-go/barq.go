package barq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	pb "github.com/YASSERRMD/barq-db/barq-sdk-go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type Config struct {
	BaseURL string
	APIKey  string
}

type Client struct {
	config Config
	http   *http.Client
}

func requireSupportedAPIVersion() error {
	version := os.Getenv("API_VERSION")
	if version == "" || version == "v1" {
		return nil
	}
	return fmt.Errorf("unsupported API_VERSION: %s", version)
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
	if err := requireSupportedAPIVersion(); err != nil {
		return err
	}
	if req.Index == nil && len(req.TextFields) == 0 {
		grpcClient, err := NewGrpcClientWithAPIKey(c.grpcTarget(), c.config.APIKey)
		if err != nil {
			return err
		}
		defer grpcClient.Close()
		return grpcClient.CreateCollection(ctx, req.Name, req.Dimension, req.Metric)
	}

	_, err := c.request(ctx, "POST", "/collections", req)
	return err
}

type InsertRequest struct {
	ID      interface{}     `json:"id"`
	Vector  []float32       `json:"vector"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Options *InsertOptions  `json:"options,omitempty"`
}

type InsertOptions struct {
	WaitForCommit *bool
}

type InsertState string

const (
	InsertStateQueued     InsertState = "queued"
	InsertStateProcessing InsertState = "processing"
	InsertStateSucceeded  InsertState = "succeeded"
	InsertStateFailed     InsertState = "failed"
)

type InsertStatus struct {
	RequestID    string
	State        InsertState
	ErrorMessage string
}

func (c *Client) Insert(ctx context.Context, collection string, req InsertRequest) error {
	if err := requireSupportedAPIVersion(); err != nil {
		return err
	}
	grpcClient, err := NewGrpcClientWithAPIKey(c.grpcTarget(), c.config.APIKey)
	if err != nil {
		return err
	}
	defer grpcClient.Close()
	return grpcClient.InsertWithOptions(
		ctx,
		collection,
		req.ID,
		req.Vector,
		rawPayloadToAny(req.Payload),
		req.Options,
	)
}

func (c *Client) InsertAsync(ctx context.Context, collection string, req InsertRequest) (string, error) {
	if err := requireSupportedAPIVersion(); err != nil {
		return "", err
	}
	grpcClient, err := NewGrpcClientWithAPIKey(c.grpcTarget(), c.config.APIKey)
	if err != nil {
		return "", err
	}
	defer grpcClient.Close()
	return grpcClient.InsertAsync(
		ctx,
		collection,
		req.ID,
		req.Vector,
		rawPayloadToAny(req.Payload),
		req.Options,
	)
}

func (c *Client) GetInsertStatus(ctx context.Context, requestID string) (InsertStatus, error) {
	if err := requireSupportedAPIVersion(); err != nil {
		return InsertStatus{}, err
	}
	grpcClient, err := NewGrpcClientWithAPIKey(c.grpcTarget(), c.config.APIKey)
	if err != nil {
		return InsertStatus{}, err
	}
	defer grpcClient.Close()
	return grpcClient.GetInsertStatus(ctx, requestID)
}

type SearchRequest struct {
	Vector  []float32      `json:"vector,omitempty"`
	Query   string         `json:"query,omitempty"`
	TopK    int            `json:"top_k"`
	Filter  interface{}    `json:"filter,omitempty"`
	Options *SearchOptions `json:"options,omitempty"`
}

type Consistency string

const (
	ConsistencyPrimary   Consistency = "primary"
	ConsistencyFollowers Consistency = "followers"
	ConsistencyAny       Consistency = "any"
)

type SearchOptions struct {
	Consistency   *Consistency
	AllowFallback *bool
}

type SearchResponse struct {
	Results []SearchResult `json:"results"`
}

type SearchResult struct {
	ID    interface{} `json:"id"`
	Score float32     `json:"score"`
}

func (c *Client) Search(ctx context.Context, collection string, req SearchRequest) ([]SearchResult, error) {
	if err := requireSupportedAPIVersion(); err != nil {
		return nil, err
	}
	if req.Query == "" && req.Filter == nil {
		grpcClient, err := NewGrpcClientWithAPIKey(c.grpcTarget(), c.config.APIKey)
		if err != nil {
			return nil, err
		}
		defer grpcClient.Close()

		results, err := grpcClient.SearchWithOptions(ctx, collection, req.Vector, req.TopK, req.Options)
		if err != nil {
			return nil, err
		}
		return results, nil
	}

	if req.Options != nil {
		return nil, errors.New("advanced search options are only supported for vector-only gRPC search")
	}

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

// gRPC Client

type GrpcClient struct {
	conn   *grpc.ClientConn
	client pb.BarqClient
	apiKey string
}

func NewGrpcClient(target string) (*GrpcClient, error) {
	return NewGrpcClientWithAPIKey(target, "")
}

func NewGrpcClientWithAPIKey(target string, apiKey string) (*GrpcClient, error) {
	conn, err := grpc.Dial(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	client := pb.NewBarqClient(conn)
	return &GrpcClient{conn: conn, client: client, apiKey: apiKey}, nil
}

func (c *GrpcClient) Close() error {
	return c.conn.Close()
}

func (c *GrpcClient) authContext(ctx context.Context) context.Context {
	if c.apiKey == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "x-api-key", c.apiKey)
}

func (c *GrpcClient) Status(ctx context.Context) (bool, error) {
	resp, err := c.client.Status(c.authContext(ctx), &pb.StatusRequest{})
	if err != nil {
		return false, err
	}
	return resp.Ok, nil
}

func (c *GrpcClient) Health(ctx context.Context) (bool, error) {
	return c.Status(ctx)
}

func (c *GrpcClient) CreateCollection(ctx context.Context, name string, dimension int, metric string) error {
	_, err := c.client.CreateCollection(c.authContext(ctx), &pb.CreateCollectionRequest{
		Name:      name,
		Dimension: uint32(dimension),
		Metric:    metric,
	})
	return err
}

func (c *GrpcClient) Insert(ctx context.Context, collection string, id interface{}, vector []float32, payload interface{}) error {
	return c.InsertWithOptions(ctx, collection, id, vector, payload, nil)
}

func (c *GrpcClient) InsertAsync(ctx context.Context, collection string, id interface{}, vector []float32, payload interface{}, options *InsertOptions) (string, error) {
	idStr := fmt.Sprintf("%v", id)

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	resp, err := c.client.InsertAsync(c.authContext(ctx), &pb.InsertRequest{
		Collection:  collection,
		Id:          idStr,
		Vector:      vector,
		PayloadJson: string(payloadBytes),
		Options:     protoInsertOptions(options),
	})
	if err != nil {
		return "", err
	}
	return resp.RequestId, nil
}

func (c *GrpcClient) GetInsertStatus(ctx context.Context, requestID string) (InsertStatus, error) {
	resp, err := c.client.GetInsertStatus(c.authContext(ctx), &pb.GetInsertStatusRequest{
		RequestId: requestID,
	})
	if err != nil {
		return InsertStatus{}, err
	}
	return InsertStatus{
		RequestID:    resp.RequestId,
		State:        insertStateFromProto(resp.State),
		ErrorMessage: resp.ErrorMessage,
	}, nil
}

func (c *GrpcClient) InsertWithOptions(ctx context.Context, collection string, id interface{}, vector []float32, payload interface{}, options *InsertOptions) error {
	idStr := fmt.Sprintf("%v", id)

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	_, err = c.client.Insert(c.authContext(ctx), &pb.InsertRequest{
		Collection:  collection,
		Id:          idStr,
		Vector:      vector,
		PayloadJson: string(payloadBytes),
		Options:     protoInsertOptions(options),
	})
	return err
}

func (c *GrpcClient) InsertDocument(ctx context.Context, collection string, id interface{}, vector []float32, payload interface{}) error {
	return c.Insert(ctx, collection, id, vector, payload)
}

func (c *GrpcClient) Search(ctx context.Context, collection string, vector []float32, topK int) ([]SearchResult, error) {
	return c.SearchWithOptions(ctx, collection, vector, topK, nil)
}

func (c *GrpcClient) SearchWithOptions(ctx context.Context, collection string, vector []float32, topK int, options *SearchOptions) ([]SearchResult, error) {
	resp, err := c.client.Search(c.authContext(ctx), &pb.SearchRequest{
		Collection: collection,
		Vector:     vector,
		TopK:       uint32(topK),
		Options:    protoSearchOptions(options),
	})
	if err != nil {
		return nil, err
	}

	var results []SearchResult
	for _, r := range resp.Results {
		id := interface{}(map[string]interface{}{"Str": r.Id})
		if numericID, err := parseCompatID(r.Id); err == nil {
			id = map[string]interface{}{"U64": numericID}
		}
		results = append(results, SearchResult{
			ID:    id,
			Score: r.Score, // Proto definition must enable Score
		})
	}
	return results, nil
}

func (c *Client) grpcTarget() string {
	if override := os.Getenv("BARQ_GRPC_ADDR"); override != "" {
		if strings.Contains(override, "://") {
			if parsed, err := url.Parse(override); err == nil && parsed.Host != "" {
				return parsed.Host
			}
		}
		return override
	}

	parsed, err := url.Parse(c.config.BaseURL)
	if err != nil || parsed.Host == "" {
		return c.config.BaseURL
	}
	parsed.Path = ""
	parsed.RawPath = ""
	parsed.RawQuery = ""
	parsed.Fragment = ""
	host := parsed.Hostname()
	if host == "" {
		return c.config.BaseURL
	}
	return fmt.Sprintf("%s:%d", host, 50051)
}

func protoInsertOptions(options *InsertOptions) *pb.InsertOptions {
	if options == nil || options.WaitForCommit == nil {
		return nil
	}
	return &pb.InsertOptions{
		WaitForCommit: *options.WaitForCommit,
	}
}

func protoSearchOptions(options *SearchOptions) *pb.SearchOptions {
	if options == nil || (options.Consistency == nil && options.AllowFallback == nil) {
		return nil
	}

	consistency := pb.Consistency_CONSISTENCY_UNSPECIFIED
	if options.Consistency != nil {
		switch *options.Consistency {
		case ConsistencyPrimary:
			consistency = pb.Consistency_CONSISTENCY_PRIMARY
		case ConsistencyFollowers:
			consistency = pb.Consistency_CONSISTENCY_FOLLOWERS
		case ConsistencyAny:
			consistency = pb.Consistency_CONSISTENCY_ANY
		default:
			consistency = pb.Consistency_CONSISTENCY_UNSPECIFIED
		}
	}

	allowFallback := true
	if options.AllowFallback != nil {
		allowFallback = *options.AllowFallback
	}

	return &pb.SearchOptions{
		Consistency:   consistency,
		AllowFallback: allowFallback,
	}
}

func insertStateFromProto(state pb.InsertStatusState) InsertState {
	switch state {
	case pb.InsertStatusState_INSERT_STATUS_STATE_QUEUED:
		return InsertStateQueued
	case pb.InsertStatusState_INSERT_STATUS_STATE_PROCESSING:
		return InsertStateProcessing
	case pb.InsertStatusState_INSERT_STATUS_STATE_SUCCEEDED:
		return InsertStateSucceeded
	case pb.InsertStatusState_INSERT_STATUS_STATE_FAILED:
		return InsertStateFailed
	default:
		return InsertStateQueued
	}
}

func rawPayloadToAny(payload json.RawMessage) interface{} {
	if len(payload) == 0 {
		return map[string]interface{}{}
	}

	var decoded interface{}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return string(payload)
	}
	return decoded
}

func parseCompatID(id string) (uint64, error) {
	var value uint64
	if _, err := fmt.Sscanf(id, "%d", &value); err != nil {
		return 0, err
	}
	if fmt.Sprintf("%d", value) != id {
		return 0, errors.New("not a numeric id")
	}
	return value, nil
}
