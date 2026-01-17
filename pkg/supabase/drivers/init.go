package drivers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/rosfandy/supago/internal/config"
)

type Supabase struct {
	Url     string
	Query   string
	Payload interface{}
	Headers map[string]string
	Config  *config.Config
}

func NewSupabase(c *config.Config) *Supabase {
	return &Supabase{
		Url: c.SupabaseUrl(),
		Headers: map[string]string{
			"apikey":        c.SupabaseApiKey,
			"Authorization": fmt.Sprintf("Bearer %s", c.SupabaseAnonKey),
			"Content-Type":  "application/json",
		},
		Config: c,
	}
}

func (s *Supabase) SetUrl(url string) *Supabase {
	s.Url = url
	return s
}

func (s *Supabase) SetPayload(payload interface{}) *Supabase {
	s.Payload = payload
	return s
}

func (s *Supabase) AddHeader(key, value string) *Supabase {
	if s.Headers == nil {
		s.Headers = make(map[string]string)
	}
	s.Headers[key] = value
	return s
}

func (s *Supabase) Read() ([]byte, error) {
	req, err := http.NewRequest("GET", s.Url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	for key, value := range s.Headers {
		req.Header.Set(key, value)
	}

	return commit(req)
}

func (s *Supabase) Write() ([]byte, error) {
	var reqBody io.Reader

	if s.Payload != nil {
		jsonData, err := json.Marshal(s.Payload)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal payload: %w", err)
		}
		reqBody = bytes.NewBuffer(jsonData)
	}

	req, err := http.NewRequest("POST", s.Url, reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	for key, value := range s.Headers {
		req.Header.Set(key, value)
	}

	return commit(req)
}

func (s *Supabase) Update() ([]byte, error) {
	var reqBody io.Reader

	if s.Payload != nil {
		jsonData, err := json.Marshal(s.Payload)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal payload: %w", err)
		}
		reqBody = bytes.NewBuffer(jsonData)
	}

	req, err := http.NewRequest("PATCH", s.Url, reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	for key, value := range s.Headers {
		req.Header.Set(key, value)
	}

	return commit(req)
}

func (s *Supabase) Delete() ([]byte, error) {
	req, err := http.NewRequest("DELETE", s.Url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	for key, value := range s.Headers {
		req.Header.Set(key, value)
	}

	return commit(req)
}

func (s *Supabase) ExecuteSQL(query string) ([]byte, error) {
	url := fmt.Sprintf("%s/database/query", s.Config.SupabaseManagementUrl())

	payload := map[string]interface{}{
		"query": query,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", s.Config.SupabaseAccessToken))
	req.Header.Set("Content-Type", "application/json")

	return commit(req)
}

func commit(req *http.Request) ([]byte, error) {
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(body))
	}

	return body, nil
}
