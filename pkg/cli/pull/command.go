package pull

import (
	"fmt"
	"go/format"
	"os"
	"strings"

	"github.com/iancoleman/strcase"
	"github.com/rosfandy/supago/internal/config"
	"github.com/rosfandy/supago/pkg/supabase/drivers"
	"github.com/rosfandy/supago/pkg/supabase/query"
)

func Run(name *string) (*query.TableSchemaResult, error) {
	cfg, err := config.LoadConfig(nil)
	if err != nil {
		return nil, fmt.Errorf("load config failed: %w", err)
	}

	d := drivers.NewSupabase(cfg)
	q := query.NewTableSchemaQuery(d)

	result, err := q.GetTableSchema(name)
	if err != nil {
		return nil, fmt.Errorf("failed to get table schema: %w", err)
	}

	if result == nil {
		return nil, fmt.Errorf("result is nil")
	}

	if err := generateStructModel(result); err != nil {
		fmt.Println("fatal: failed to generate struct model, error:", err.Error())
		return nil, err
	}

	return result, nil
}

func Setup() error {
	cfg, err := config.LoadConfig(nil)
	if err != nil {
		return fmt.Errorf("load config failed: %w", err)
	}

	if cfg.SupabaseAccessToken == "" {
		return fmt.Errorf("SUPABASE_ACCESS_TOKEN is required for setup. Get it from: https://supabase.com/dashboard/account/tokens")
	}

	if cfg.SupabaseProjectId == "" {
		return fmt.Errorf("SUPABASE_PROJECT_ID is required")
	}

	d := drivers.NewSupabase(cfg)
	q := query.NewTableSchemaQuery(d)

	fmt.Println("Checking database setup...")
	fmt.Println("Checking existing functions...")

	schemaViewExists, err := q.CheckFunctionExistsInDB("get_table_schema")
	if err != nil {
		fmt.Printf("Warning: Could not check get_table_schema existence: %v\n", err)
		fmt.Println("   Proceeding with setup...")
		schemaViewExists = false
	}

	execSqlExists, err := q.CheckFunctionExistsInDB("exec_sql")
	if err != nil {
		fmt.Printf("Warning: Could not check exec_sql existence: %v\n", err)
		fmt.Println("   Proceeding with setup...")
		execSqlExists = false
	}

	if schemaViewExists && execSqlExists {
		fmt.Println("\nAll database functions already exist!")
		fmt.Println("\nExisting functions:")
		fmt.Println("  • get_table_schema(p_table_name TEXT)")
		fmt.Println("  • exec_sql(query TEXT)")
		fmt.Println("\nNo action needed. You can run: supago pull <table_name>")
		return nil
	}

	fmt.Println("\nInitializing database functions...")

	if schemaViewExists {
		fmt.Println("get_table_schema already exists, skipping...")
	}

	if execSqlExists {
		fmt.Println("exec_sql already exists, skipping...")
	}

	err = q.InitializeDatabaseSelective(schemaViewExists, execSqlExists)
	if err != nil {
		fmt.Println("fatal:", err.Error())
		return err
	}

	fmt.Println("Database functions created successfully!")

	if !schemaViewExists || !execSqlExists {
		fmt.Println("\nFunctions created:")
		if !schemaViewExists {
			fmt.Println("  • get_table_schema(p_table_name TEXT)")
		}
		if !execSqlExists {
			fmt.Println("  • exec_sql(query TEXT)")
		}
	}

	fmt.Println("\nYou can now run: supago pull <table_name>")

	return nil
}

func EnsureFunctions() error {
	cfg, err := config.LoadConfig(nil)
	if err != nil {
		return err
	}

	d := drivers.NewSupabase(cfg)
	q := query.NewTableSchemaQuery(d)

	exists, err := q.CheckFunctionExistsInDB("get_table_schema")
	if err != nil {
		return nil
	}

	if !exists {
		fmt.Println("Warning: get_table_schema function not found")
		fmt.Println("   Views will be created using Management API directly")
		fmt.Println("   Run 'supago pull setup' to create the function")
	}

	return nil
}

func CheckSetup() error {
	cfg, err := config.LoadConfig(nil)
	if err != nil {
		return err
	}

	d := drivers.NewSupabase(cfg)
	q := query.NewTableSchemaQuery(d)

	fmt.Println("Checking database setup...")

	exists, err := q.CheckFunctionExistsInDB("get_table_schema")
	if err != nil {
		fmt.Println("Failed to check get_table_schema function")
		return err
	}

	if exists {
		fmt.Println("get_table_schema function exists")
	} else {
		fmt.Println("get_table_schema function not found")
		fmt.Println("   Run: supago pull setup")
		return fmt.Errorf("database not properly set up")
	}

	execExists, err := q.CheckFunctionExistsInDB("exec_sql")
	if err == nil && execExists {
		fmt.Println("exec_sql function exists")
	} else {
		fmt.Println("exec_sql function not found (optional)")
	}

	fmt.Println("\nDatabase setup is complete!")
	return nil
}

func generateStructModel(result *query.TableSchemaResult) error {
	outputDir := "internal/domain"
	packageName := "domain"

	tableName := strcase.ToCamel(result.TableName)
	var structModel strings.Builder

	fmt.Printf("\nTable: %s\n", tableName)

	fmt.Fprint(&structModel, "package "+packageName+"\n\n")
	structModel.WriteString("import \"time\"\n\n")
	fmt.Fprintf(&structModel, "type %s struct {\n", tableName)

	fmt.Println("Columns:")
	for _, col := range result.Columns {
		fieldName := strcase.ToCamel(col.ColumnName)
		fieldType := pgToGoType(col.DataType, col.IsNullable)

		fmt.Fprintf(
			&structModel,
			"\t%s %s `db:\"%s\" json:\"%s\"`\n",
			fieldName, fieldType, col.ColumnName, col.ColumnName,
		)

		nullable := "NOT NULL"
		if col.IsNullable {
			nullable = "NULL"
		}
		defaultVal := col.ColumnDefault
		if defaultVal == "" {
			defaultVal = "-"
		}
		fmt.Printf("  • %-20s %-15s %-10s default: %s\n",
			col.ColumnName, col.DataType, nullable, defaultVal)
	}
	structModel.WriteString("}\n")

	src, err := format.Source([]byte(structModel.String()))
	if err != nil {
		return err
	}

	if err := os.MkdirAll(outputDir, os.ModePerm); err != nil {
		return err
	}

	fmt.Println("\nGenerated model:", fmt.Sprintf("%s/%s.go", outputDir, strings.ToLower(result.TableName)))
	return os.WriteFile(fmt.Sprintf("%s/%s.go", outputDir, strings.ToLower(result.TableName)), src, 0644)
}

func pgToGoType(pgType string, nullable bool) string {
	var t string

	switch pgType {
	case "uuid", "text", "varchar", "character varying":
		t = "string"
	case "int2", "int4", "integer", "smallint":
		t = "int"
	case "int8", "bigint":
		t = "int64"
	case "bool", "boolean":
		t = "bool"
	case "timestamp", "timestamp without time zone", "timestamptz", "timestamp with time zone":
		t = "time.Time"
	case "date":
		t = "time.Time"
	case "numeric", "decimal", "float4", "float8":
		t = "float64"
	case "json", "jsonb":
		t = "map[string]any"
	default:
		t = "any"
	}

	if nullable {
		return "*" + t
	}
	return t
}
