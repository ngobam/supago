package commands

import (
	"github.com/rosfandy/supago/pkg/cli/server"
	"github.com/spf13/cobra"
)

func ServeCommands() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "server",
		Short: "Start Supago server",
		Long:  "Start Supago server",
		Run: func(cmd *cobra.Command, args []string) {
			_ = cmd // unused

			server.Run()

			select {}
		},
	}

	return cmd
}
