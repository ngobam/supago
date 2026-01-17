package main

import (
	"os"

	"github.com/rosfandy/supago/cmd/commands"
)

func main() {
	cmd := commands.InitCommands()

	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
