package main

import (
	"db/dbcli"
	"db/server"

	"github.com/spf13/cobra"
)

var RootCmd = &cobra.Command{
	Use:   "cli",
	Short: "CLI for NutellaDB",
	Long:  "A Command Line Interface (CLI) for managing collections, version control and server on NutellaDB",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) > 0 && args[0] == "startserver" {
			server.Server(cmd)
		} else {
			dbcli.Execute()
		}
	},
}

func main() {
	RootCmd.Execute()
}
