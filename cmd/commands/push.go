package commands

import (
	"fmt"
	"os"

	"github.com/rosfandy/supago/pkg/cli/push"
	"github.com/spf13/cobra"
)

func PushCommands() *cobra.Command {
	var path string

	cmd := &cobra.Command{
		Use:   "push <table_name>",
		Short: "Push table schema to supabase",
		Long:  "Push table schema to supabase",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return fmt.Errorf(
					"table_name is required\n\nUsage:\n  supago push <table_name> [--path path]\n\nExample:\n  supago push examples --path internal/domain",
				)
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			tableName := args[0]

			if path == "" {
				path = "internal/domain"
			}

			if err := push.Run(tableName, path); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		},
	}

	cmd.Flags().StringVar(
		&path,
		"path",
		"internal/domain",
		"Directory for table schema",
	)

	return cmd
}
