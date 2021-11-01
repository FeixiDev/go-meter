package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// compareCmd represents the compare command
var compareCmd = &cobra.Command{
	Use:   "compare",
	Short: "compare",
	Long:  `Compare`,
	Args:  cobra.NoArgs,
	// Aliases: []string{"cp"},
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("compareCmd")
	},
}

func init() {
	rootCmd.AddCommand(compareCmd)
}
