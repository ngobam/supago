package query

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/rosfandy/supago/pkg/supabase/drivers"
	"github.com/rosfandy/supago/pkg/supabase/function"
)

type ColumnSchema struct {
	ColumnName    string `json:"column_name"`
	DataType      string `json:"data_type"`
	IsNullable    bool   `json:"is_nullable"`
	ColumnDefault string `json:"column_default"`
}

type TableSchemaResult struct {
	TableName string         `json:"table_name"`
	Columns   []ColumnSchema `json:"columns"`
}

type SupabaseQuery struct {
	*drivers.Supabase
}

func NewTableSchemaQuery(d *drivers.Supabase) *SupabaseQuery {
	return &SupabaseQuery{
		Supabase: d,
	}
}

// Helper method untuk clone instance dengan headers
func (sq *SupabaseQuery) clone() *SupabaseQuery {
	newHeaders := make(map[string]string)
	for k, v := range sq.Headers {
		newHeaders[k] = v
	}

	return &SupabaseQuery{
		Supabase: &drivers.Supabase{
			Url:     sq.Url,
			Headers: newHeaders,
			Config:  sq.Config,
		},
	}
}

func (s *SupabaseQuery) GetTableSchema(tableName *string) (*TableSchemaResult, error) {
	if tableName == nil || *tableName == "" {
		return nil, fmt.Errorf("table name cannot be empty")
	}

	exists, err := s.checkSchemaViewExists(tableName)
	if err != nil {
		return nil, err
	}

	if !exists {
		if err := s.createSchemaView(tableName); err != nil {
			return nil, err
		}
	}

	columns, err := s.getSchemaFromView(tableName)
	if err != nil {
		return nil, err
	}

	result := &TableSchemaResult{
		TableName: *tableName,
		Columns:   columns,
	}

	return result, nil
}

func (s *SupabaseQuery) checkSchemaViewExists(tableName *string) (bool, error) {
	viewName := *tableName + "_schema"

	sq := s.clone()
	_, err := sq.From(viewName).
		Select("column_name,data_type,is_nullable,column_default").
		Read()

	if err != nil {
		if strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "Could not find") {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

// createSchemaView creates a view for table schema using Management API
func (s *SupabaseQuery) createSchemaView(tableName *string) error {
	viewName := *tableName + "_schema"

	createViewSQL := fmt.Sprintf(`
CREATE OR REPLACE VIEW public.%s AS
SELECT
	column_name,
	data_type,
	(is_nullable = 'YES')::boolean as is_nullable,
	COALESCE(column_default, '') as column_default
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = '%s'
ORDER BY ordinal_position;

GRANT SELECT ON public.%s TO anon, authenticated;
	`, viewName, *tableName, viewName)

	sq := s.clone()
	body, err := sq.ExecuteSQL(createViewSQL)
	if err != nil {
		return fmt.Errorf("failed to create view via Management API: %w", err)
	}

	// Parse response
	var result []map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		// Management API might return different response format
		fmt.Println("View creation response:", string(body))
	}

	return nil
}

func (s *SupabaseQuery) getSchemaFromView(tableName *string) ([]ColumnSchema, error) {
	viewName := *tableName + "_schema"

	sq := s.clone()
	body, err := sq.From(viewName).Select("*").Read()

	if err != nil {
		return nil, err
	}

	var columns []ColumnSchema
	if err := json.Unmarshal(body, &columns); err != nil {
		return nil, fmt.Errorf("failed to parse columns: %w", err)
	}

	return columns, nil
}

func (s *SupabaseQuery) GetAllTableSchemas() ([]TableSchemaResult, error) {
	// Get all table names from information_schema.tables
	sq := s.clone()
	body, err := sq.From("information_schema.tables").
		Select("table_name").
		Eq("table_schema", "public").
		Eq("table_type", "BASE TABLE").
		Order("table_name", true).
		Read()

	if err != nil {
		return nil, fmt.Errorf("failed to get table names: %w", err)
	}

	var tables []struct {
		TableName string `json:"table_name"`
	}
	if err := json.Unmarshal(body, &tables); err != nil {
		return nil, fmt.Errorf("failed to parse table names: %w", err)
	}

	var results []TableSchemaResult
	for _, table := range tables {
		schema, err := s.GetTableSchema(&table.TableName)
		if err != nil {
			fmt.Printf("Warning: failed to get schema for table %s: %v\n", table.TableName, err)
			continue
		}

		results = append(results, *schema)
	}

	return results, nil
}

// DropSchemaView drops a schema view if it exists using Management API
func (s *SupabaseQuery) DropSchemaView(tableName *string) error {
	viewName := *tableName + "_schema"

	dropSQL := fmt.Sprintf("DROP VIEW IF EXISTS public.%s CASCADE;", viewName)

	sq := s.clone()
	_, err := sq.ExecuteSQL(dropSQL)
	if err != nil {
		return fmt.Errorf("failed to drop view: %w", err)
	}

	return nil
}

// RefreshSchemaView recreates the schema view
func (s *SupabaseQuery) RefreshSchemaView(tableName *string) error {
	if err := s.DropSchemaView(tableName); err != nil {
		return err
	}

	// Recreate view
	return s.createSchemaView(tableName)
}

// GetTableSchemaViaRPC gets schema using RPC function (alternative method)
// This requires the get_table_schema RPC function to be created in Supabase
func (s *SupabaseQuery) GetTableSchemaViaRPC(tableName *string) (*TableSchemaResult, error) {
	params := map[string]interface{}{
		"p_table_name": *tableName,
	}

	sq := s.clone()
	body, err := sq.RPC("get_table_schema", params).Write()
	if err != nil {
		return nil, err
	}

	var result TableSchemaResult
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse schema: %w", err)
	}

	return &result, nil
}

// GetAllTableSchemasViaRPC gets all schemas using RPC function (alternative method)
// This requires the get_all_table_schemas RPC function to be created in Supabase
func (s *SupabaseQuery) GetAllTableSchemasViaRPC() ([]TableSchemaResult, error) {
	sq := s.clone()
	body, err := sq.RPC("get_all_table_schemas", nil).Write()
	if err != nil {
		return nil, err
	}

	var results []TableSchemaResult
	if err := json.Unmarshal(body, &results); err != nil {
		return nil, fmt.Errorf("failed to parse schemas: %w", err)
	}

	return results, nil
}

// GetTableInfo gets basic table information without creating views
func (s *SupabaseQuery) GetTableInfo(tableName *string) (*TableSchemaResult, error) {
	if tableName == nil || *tableName == "" {
		return nil, fmt.Errorf("table name cannot be empty")
	}

	sq := s.clone()
	body, err := sq.From("information_schema.columns").
		Select("column_name,data_type,is_nullable,column_default").
		Eq("table_schema", "public").
		Eq("table_name", *tableName).
		Order("ordinal_position", true).
		Read()

	if err != nil {
		return nil, err
	}

	var rawColumns []struct {
		ColumnName    string  `json:"column_name"`
		DataType      string  `json:"data_type"`
		IsNullable    string  `json:"is_nullable"`
		ColumnDefault *string `json:"column_default"`
	}

	if err := json.Unmarshal(body, &rawColumns); err != nil {
		return nil, fmt.Errorf("failed to parse columns: %w", err)
	}

	columns := make([]ColumnSchema, len(rawColumns))
	for i, raw := range rawColumns {
		columns[i] = ColumnSchema{
			ColumnName:    raw.ColumnName,
			DataType:      raw.DataType,
			IsNullable:    raw.IsNullable == "YES",
			ColumnDefault: "",
		}
		if raw.ColumnDefault != nil {
			columns[i].ColumnDefault = *raw.ColumnDefault
		}
	}

	result := &TableSchemaResult{
		TableName: *tableName,
		Columns:   columns,
	}

	return result, nil
}

// CheckFunctionExists checks if a given RPC function exists (legacy method)
// Note: This is unreliable and kept for backward compatibility
func (s *SupabaseQuery) CheckFunctionExists(functionName string) (bool, error) {
	params := map[string]interface{}{}
	sq := s.clone()
	_, err := sq.RPC(functionName, params).Write()
	if err != nil && strings.Contains(err.Error(), "Could not find the function") {
		return false, nil
	} else if err != nil {
		// Other error, assume function exists or network issue
		return true, nil
	}
	return true, nil
}

// CheckFunctionExistsInDB checks if a function exists by querying pg_catalog
// This is the recommended way to check function existence
func (s *SupabaseQuery) CheckFunctionExistsInDB(functionName string) (bool, error) {
	query := fmt.Sprintf(`
		SELECT COUNT(*) > 0 as exists
		FROM pg_catalog.pg_proc p
		JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
		WHERE p.proname = '%s'
		AND n.nspname = 'public'
	`, functionName)

	type ExistsResult struct {
		Exists bool `json:"exists"`
	}

	sq := s.clone()
	body, err := sq.RPC("exec_sql", map[string]interface{}{"query": query}).Write()

	if err != nil {
		if strings.Contains(err.Error(), "Could not find the function") {
			return s.checkFunctionViaManagementAPI(functionName)
		}
		return s.checkFunctionViaManagementAPI(functionName)
	}

	if len(body) == 0 || string(body) == "null" || string(body) == "[]" {
		// exec_sql exists but returns void, fallback to Management API
		return s.checkFunctionViaManagementAPI(functionName)
	}

	var results []ExistsResult
	if err := json.Unmarshal(body, &results); err != nil {
		return s.checkFunctionViaManagementAPI(functionName)
	}

	if len(results) > 0 {
		return results[0].Exists, nil
	}

	return false, nil
}

// checkFunctionViaManagementAPI checks function existence via Management API
func (s *SupabaseQuery) checkFunctionViaManagementAPI(functionName string) (bool, error) {
	// Try a simpler approach: list all functions and check if ours exists
	query := `
		SELECT p.proname as function_name
		FROM pg_catalog.pg_proc p
		JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
		WHERE n.nspname = 'public'
	`

	sq := s.clone()
	body, err := sq.ExecuteSQL(query)
	if err != nil {
		return false, err
	}

	if len(body) == 0 || string(body) == "[]" {
		return false, nil
	}

	var results []map[string]interface{}
	if err := json.Unmarshal(body, &results); err != nil {
		fmt.Printf("Debug: Failed to parse response: %s\n", string(body))
		return false, nil
	}

	for _, result := range results {
		if fname, ok := result["function_name"].(string); ok {
			if fname == functionName {
				return true, nil
			}
		}
	}

	return false, nil
}

// CreateTableSchemaFunction creates the create_table_schema_view function using Management API
func (s *SupabaseQuery) CreateTableSchemaFunction() error {
	sq := s.clone()
	body, err := sq.ExecuteSQL(function.GetTableSchemaSQL)
	if err != nil {
		return fmt.Errorf("failed to create function via Management API: %w", err)
	}

	fmt.Println("Function created successfully:", string(body))
	return nil
}

// CreateExecSQLFunction creates exec_sql function using Management API
func (s *SupabaseQuery) CreateExecSQLFunction() error {
	sq := s.clone()
	body, err := sq.ExecuteSQL(function.ExecSQL)
	if err != nil {
		return fmt.Errorf("failed to create exec_sql function: %w", err)
	}

	fmt.Println("exec_sql function created successfully:", string(body))
	return nil
}

// InitializeDatabase creates all necessary functions and views
func (s *SupabaseQuery) InitializeDatabase() error {
	fmt.Println("Initializing database functions...")

	if err := s.CreateExecSQLFunction(); err != nil {
		fmt.Printf("Warning: failed to create exec_sql function: %v\n", err)
	}

	if err := s.CreateTableSchemaFunction(); err != nil {
		return fmt.Errorf("failed to create schema function: %w", err)
	}

	fmt.Println("Database initialization completed successfully!")
	return nil
}

// InitializeDatabaseSelective creates only missing database functions
func (s *SupabaseQuery) InitializeDatabaseSelective(schemaViewExists, execSqlExists bool) error {
	if !execSqlExists {
		if err := s.CreateExecSQLFunction(); err != nil {
			fmt.Printf("Warning: failed to create exec_sql function: %v\n", err)
		}
	}

	if !schemaViewExists {
		if err := s.CreateTableSchemaFunction(); err != nil {
			return fmt.Errorf("failed to create schema function: %w", err)
		}
	}

	return nil
}
