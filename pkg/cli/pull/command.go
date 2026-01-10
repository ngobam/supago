package pull

import (
	"fmt"

	"github.com/rosfandy/supago/internal/config"
	"github.com/rosfandy/supago/pkg/supabase/drivers"
	"github.com/rosfandy/supago/pkg/supabase/query"
)

func Run(name *string) (*query.TableSchemaResult, error) {
	cfg, err := config.LoadConfig(nil)
	if err != nil {
		fmt.Println("Error loading config:", err.Error())
		return nil, err
	}

	d := drivers.NewSupabase(cfg)
	q := query.NewTableSchemaQuery(d)

	result, err := q.GetTableSchema(name)
	if err != nil {
		fmt.Println("fatal: failed to get table schema, error:", err.Error())
		return nil, err
	}

	if result == nil {
		fmt.Println("fatal: Record not found")
		return nil, fmt.Errorf("result is nil")
	}

	fmt.Printf("\nTable: %s\n", result.TableName)
	fmt.Println("Columns:")
	for _, col := range result.Columns {
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

	return result, nil
}

// Setup creates all necessary database functions using Management API
func Setup() error {
	cfg, err := config.LoadConfig(nil)
	if err != nil {
		fmt.Println("Error loading config:", err.Error())
		return err
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
		fmt.Printf("⚠️  Warning: Could not check get_table_schema existence: %v\n", err)
		fmt.Println("   Proceeding with setup...")
		schemaViewExists = false
	}

	execSqlExists, err := q.CheckFunctionExistsInDB("exec_sql")
	if err != nil {
		fmt.Printf("⚠️  Warning: Could not check exec_sql existence: %v\n", err)
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

// EnsureFunctions checks if necessary functions exist (lightweight check)
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
		fmt.Println("⚠️  Warning: get_table_schema function not found")
		fmt.Println("   Views will be created using Management API directly")
		fmt.Println("   Run 'supago pull setup' to create the function")
	}

	return nil
}

// CheckSetup verifies if the database is properly set up
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
		fmt.Println("⚠️  exec_sql function not found (optional)")
	}

	fmt.Println("\nDatabase setup is complete!")
	return nil
}
