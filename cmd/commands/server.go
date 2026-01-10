package commands

import (
	"github.com/rosfandy/supago/internal/config"
	"github.com/rosfandy/supago/pkg/logger"
	"github.com/spf13/cobra"
)

var Logger = logger.HcLog().Named("supago.commands")

func ServeCommands() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "server",
		Short: "Start Supago server",
		Long:  "Start Supago server",
		Run: func(cmd *cobra.Command, args []string) {
			_ = cmd // unused
			cfg, err := config.LoadConfig(nil)
			if err != nil {
				logger.Fatal("Failed to load config", "error", err)
			}

			server := config.NewServer(cfg)
			server.RunHttpServer()

			// Block forever
			select {}
		},
	}

	return cmd
}
