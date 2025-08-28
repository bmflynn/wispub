package cmd

import (
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "wispub",
	Short: "Publisher for WIS 2.0 messages",
	Long: `Tool for publishing product messages to a WIS 2.0 MQTT broker.

See: https://community.wmo.int/activity-areas/wis/wis2-implementation
Project: https://github.com/bmflynn/wispub
`,
	Args:    cobra.NoArgs,
	Version: "",
}

func Execute(version string) error {
	rootCmd.Version = version
	return rootCmd.Execute()
}
