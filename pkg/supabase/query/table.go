package query

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/rosfandy/supago/pkg/supabase/drivers"
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

// Query builder methods untuk SupabaseQuery
func (sq *SupabaseQuery) From(tableName string) *SupabaseQuery {
	baseUrl := sq.Url
	if idx := strings.Index(sq.Url, "/rest/v1/"); idx != -1 {
		baseUrl = sq.Url[:idx]
	}

	sq.Url = fmt.Sprintf("%s/rest/v1/%s", baseUrl, tableName)
	return sq
}

func (sq *SupabaseQuery) Select(columns string) *SupabaseQuery {
	if sq.Url == "" {
		return sq
	}

	separator := sq.getSeparator()
	sq.Url = fmt.Sprintf("%s%sselect=%s", sq.Url, separator, columns)
	return sq
}

func (sq *SupabaseQuery) Eq(column, value string) *SupabaseQuery {
	if sq.Url == "" {
		return sq
	}

	separator := sq.getSeparator()
	sq.Url = fmt.Sprintf("%s%s%s=eq.%s", sq.Url, separator, column, value)
	return sq
}

func (sq *SupabaseQuery) Order(column string, ascending bool) *SupabaseQuery {
	if sq.Url == "" {
		return sq
	}

	separator := sq.getSeparator()
	direction := "desc"
	if ascending {
		direction = "asc"
	}

	sq.Url = fmt.Sprintf("%s%sorder=%s.%s", sq.Url, separator, column, direction)
	return sq
}

func (sq *SupabaseQuery) RPC(functionName string, params interface{}) *SupabaseQuery {
	baseUrl := sq.Url
	if idx := strings.Index(sq.Url, "/rest/v1/"); idx != -1 {
		baseUrl = sq.Url[:idx]
	}

	sq.Url = fmt.Sprintf("%s/rest/v1/rpc/%s", baseUrl, functionName)
	sq.Payload = params
	return sq
}

func (sq *SupabaseQuery) getSeparator() string {
	if strings.Contains(sq.Url, "?") {
		return "&"
	}
	return "?"
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
		},
	}
}

func (s *SupabaseQuery) GetTableSchema(tableName *string) (*TableSchemaResult, error) {
	if tableName == nil || *tableName == "" {
		return nil, fmt.Errorf("table name cannot be empty")
	}

	// Check if schema view exists
	exists, err := s.checkSchemaViewExists(tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to check schema view: %w", err)
	}

	// If view doesn't exist, create it
	if !exists {
		if err := s.createSchemaView(tableName); err != nil {
			return nil, fmt.Errorf("failed to create schema view: %w", err)
		}
	}

	// Get schema from view
	columns, err := s.getSchemaFromView(tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	result := &TableSchemaResult{
		TableName: *tableName,
		Columns:   columns,
	}

	return result, nil
}

func (s *SupabaseQuery) checkSchemaViewExists(tableName *string) (bool, error) {
	viewName := *tableName + "_schema"

	// Langsung coba query ke view, jika 404 berarti belum ada
	sq := s.clone()
	_, err := sq.From(viewName).
		Select("column_name,data_type,is_nullable,column_default").
		Read()

	if err != nil {
		// Jika error 404 atau "Could not find", view belum ada
		if strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "Could not find") {
			return false, nil
		}
		// Error lain
		return false, err
	}

	// Jika tidak error, view exists
	return true, nil
}

// createSchemaView creates a view for table schema
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

	params := map[string]interface{}{
		"query": createViewSQL,
	}

	sq := s.clone()
	_, err := sq.RPC("exec_sql", params).Write()
	if err != nil {
		return fmt.Errorf("failed to create view via RPC: %w", err)
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

// DropSchemaView drops a schema view if it exists
func (s *SupabaseQuery) DropSchemaView(tableName *string) error {
	viewName := *tableName + "_schema"

	dropSQL := fmt.Sprintf("DROP VIEW IF EXISTS public.%s CASCADE;", viewName)

	params := map[string]interface{}{
		"query": dropSQL,
	}

	sq := s.clone()
	_, err := sq.RPC("exec_sql", params).Write()
	if err != nil {
		return fmt.Errorf("failed to drop view: %w", err)
	}

	return nil
}

// RefreshSchemaView recreates the schema view
func (s *SupabaseQuery) RefreshSchemaView(tableName *string) error {
	// Drop existing view
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

	// Query information_schema.columns directly
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

	// Parse columns - need to handle is_nullable as string first
	var rawColumns []struct {
		ColumnName    string  `json:"column_name"`
		DataType      string  `json:"data_type"`
		IsNullable    string  `json:"is_nullable"`
		ColumnDefault *string `json:"column_default"`
	}

	if err := json.Unmarshal(body, &rawColumns); err != nil {
		return nil, fmt.Errorf("failed to parse columns: %w", err)
	}

	// Convert to ColumnSchema
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
