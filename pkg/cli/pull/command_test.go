package pull

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/rosfandy/supago/pkg/supabase/query"
)

func setupMockServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/rest/v1/rpc/exec_sql":
			response := []map[string]interface{}{
				{"exists": true},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(response)
		case "/rest/v1/blogs_schema":
			columns := []query.ColumnSchema{
				{
					ColumnName:    "id",
					DataType:      "uuid",
					IsNullable:    false,
					ColumnDefault: "gen_random_uuid()",
				},
				{
					ColumnName:    "title",
					DataType:      "text",
					IsNullable:    false,
					ColumnDefault: "",
				},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(columns)
		default:
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, "404 Not Found")
		}
	}))
}

func setupTestEnv(t *testing.T) (*httptest.Server, func()) {
	server := setupMockServer()

	os.Setenv("SUPABASE_PROJECT_ID", "test-project")
	os.Setenv("SUPABASE_API_KEY", "test-api-key")
	os.Setenv("SUPABASE_ANON_KEY", "test-anon-key")
	os.Setenv("SUPABASE_ACCESS_TOKEN", "test-access-token")
	os.Setenv("SERVER_HOST", "localhost")
	os.Setenv("SERVER_PORT", "8080")
	os.Setenv("MAX_SERVER_REQUEST_BODY_SIZE", "1048576")

	configContent := `SERVER_HOST: "localhost"
SERVER_PORT: "8080"
SUPABASE_PROJECT_ID: "test-project"
SUPABASE_API_KEY: "test-api-key"
SUPABASE_ANON_KEY: "test-anon-key"
SUPABASE_ACCESS_TOKEN: "test-access-token"
MAX_SERVER_REQUEST_BODY_SIZE: 1048576
`

	err := os.WriteFile("app.yaml", []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create app.yaml: %v", err)
	}

	cleanup := func() {
		os.Unsetenv("SUPABASE_PROJECT_ID")
		os.Unsetenv("SUPABASE_API_KEY")
		os.Unsetenv("SUPABASE_ANON_KEY")
		os.Unsetenv("SUPABASE_ACCESS_TOKEN")
		os.Unsetenv("SERVER_HOST")
		os.Unsetenv("SERVER_PORT")
		os.Unsetenv("MAX_SERVER_REQUEST_BODY_SIZE")
		os.Remove("app.yaml")
		server.Close()
	}

	return server, cleanup
}

func TestRun_ConfigLoadError(t *testing.T) {
	os.Remove("app.yaml")

	result, err := Run(stringPtr("blogs"))

	if err == nil {
		t.Error("Expected error when config file doesn't exist")
	}

	if result != nil {
		t.Error("Expected nil result when config load fails")
	}
}

func stringPtr(s string) *string {
	return &s
}

func TestSetup_MissingAccessToken(t *testing.T) {
	server, cleanup := setupTestEnv(t)
	defer cleanup()
	_ = server
	configContent := `SERVER_HOST: "localhost"
SERVER_PORT: "8080"
SUPABASE_PROJECT_ID: "test-project"
SUPABASE_API_KEY: "test-api-key"
SUPABASE_ANON_KEY: "test-anon-key"
SUPABASE_ACCESS_TOKEN: ""
MAX_SERVER_REQUEST_BODY_SIZE: 1048576
`

	err := os.WriteFile("app.yaml", []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create app.yaml: %v", err)
	}

	err = Setup()
	if err == nil {
		t.Error("Expected error for missing access token")
	}

	expectedMsg := "SUPABASE_ACCESS_TOKEN is required for setup. Get it from: https://supabase.com/dashboard/account/tokens"
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
	}
}

func TestEnsureFunctions_ConfigError(t *testing.T) {
	os.Remove("app.yaml")

	err := EnsureFunctions()
	if err == nil {
		t.Error("Expected error when config cannot be loaded")
	}
}

func TestCheckSetup_ConfigError(t *testing.T) {
	os.Remove("app.yaml")

	err := CheckSetup()
	if err == nil {
		t.Error("Expected error when config cannot be loaded")
	}
}
