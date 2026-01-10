package main

import (
	"os"

	"github.com/rosfandy/supago/cmd/commands"
	"github.com/spf13/cobra"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "supago",
		Short: "Supago CLI",
	}

	rootCmd.AddCommand(commands.ServeCommands())

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
