package cmd

import (
	"fmt"
	"go-meter/pipeline"
	"strconv"

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
		fmt.Println("Start to compare files...")
		checkInputArgs()
		jobNum := InputArgs.JobNum
		masterBlock := pipeline.MasterBlockInit()
		CompareFiles(masterBlock, jobNum)
		fmt.Println("Finish to compare files...")
	},
}

func init() {
	rootCmd.AddCommand(compareCmd)
}

func CompareFiles(masterBlock *[]uint64, jobNum uint) {
	fileSize, _ := strconv.Atoi(InputArgs.TotalSize)
	blockSize, _ := strconv.Atoi(InputArgs.BlockSize)

	dev := pipeline.NewDevice(InputArgs.DevicePath, fileSize, blockSize, int(jobNum), 0, InputArgs.MasterMask, masterBlock)
	dev.CompareDevice()
}
