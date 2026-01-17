package commands

import (
	"fmt"
	"os"

	"github.com/rosfandy/supago/pkg/cli/pull"
	"github.com/spf13/cobra"
)

func PullCommands() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "pull <table_name>",
		Short:   "Pull Supabase table schema",
		Long:    "Pull table schema from Supabase and display column information",
		Example: `supago pull profiles`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return fmt.Errorf("table_name is required\n\nUsage:\n  supago pull <table_name>\n\nExample:\n  supago pull blogs")
			}
			return nil
		},
		PreRun: func(cmd *cobra.Command, args []string) {
			if err := pull.EnsureFunctions(); err != nil {
				fmt.Println("Warning:", err)
			}
		},
		Run: func(cmd *cobra.Command, args []string) {
			tableName := args[0]
			result, err := pull.Run(&tableName)
			if err != nil {
				os.Exit(1)
			}
			if result == nil {
				fmt.Println("❌ Failed to get table schema")
				os.Exit(1)
			}
		},
	}

	setupCmd := &cobra.Command{
		Use:     "setup",
		Short:   "Setup database functions",
		Long:    "Create necessary database functions for schema operations using Management API",
		Example: `  supago pull setup`,
		Run: func(cmd *cobra.Command, args []string) {
			if err := pull.Setup(); err != nil {
				fmt.Println("\n❌ Setup failed:", err.Error())
				fmt.Println("\nTroubleshooting:")
				fmt.Println("1. Make sure SUPABASE_ACCESS_TOKEN is set in app.yaml")
				fmt.Println("2. Get your token from: https://supabase.com/dashboard/account/tokens")
				fmt.Println("3. Make sure SUPABASE_PROJECT_REF is correct")
				os.Exit(1)
			}
		},
	}

	checkCmd := &cobra.Command{
		Use:     "check",
		Short:   "Check database setup",
		Long:    "Verify that all required database functions are properly set up",
		Example: `  supago pull check`,
		Run: func(cmd *cobra.Command, args []string) {
			if err := pull.CheckSetup(); err != nil {
				os.Exit(1)
			}
		},
	}

	cmd.AddCommand(setupCmd)
	cmd.AddCommand(checkCmd)

	return cmd
}
