package commands

import (
	"fmt"

	"github.com/rosfandy/supago/pkg/cli/pull"
	"github.com/spf13/cobra"
)

func PullCommands() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "pull <table_name>",
		Short:   "Pull Supabase Model",
		Long:    "Pull Supabase schema from a specific table",
		Example: `supago pull blogs`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return fmt.Errorf("table_name is required\n\nUsage:\n  supago pull <table_name>\n\nExample:\n  supago pull blogs")
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			tableName := args[0]
			pull.Run(&tableName)
		},
	}

	return cmd
}
