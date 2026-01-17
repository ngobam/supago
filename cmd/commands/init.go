package commands

import "github.com/spf13/cobra"

func InitCommands() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "supago",
		Short: "Supago CLI",
	}

	cmd.AddCommand(ServeCommands())
	cmd.AddCommand(PullCommands())
	cmd.AddCommand(PushCommands())

	return cmd
}
