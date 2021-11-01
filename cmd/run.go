/*
 */
package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// var Lineage string

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "copy and compare",
	Long:  `Copy and Compare`,
	Args:  cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("runCmd")
		// if len(Lineage) != 0 {
		// 	fmt.Println("Lineage", Lineage)
		// }
		// fmt.Println("args", args)
	},
}

func init() {
	rootCmd.AddCommand(runCmd)
}
