package cmd

import (
	"fmt"
	"go-meter/pipeline"
	"strconv"

	// "github.com/robfig/cron"
	"github.com/spf13/cobra"
)

var WriteDeviceCmd = &cobra.Command{
	Use:   "write",
	Short: "write",
	Long:  `WriteDevice`,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Start to write files...")
		checkBlockArgs()

		// c := cron.New()
		// c.AddFunc("@every 1s", func() {
		// 	printPerfor()
		// })
		// c.Start()

		masterBlock := pipeline.MasterBlockInit()

		jobNum := InputArgs.JobNum
		WriteDevice(masterBlock, jobNum)

		fmt.Println("Finish to write files...")
	},
}

func init() {
	rootCmd.AddCommand(WriteDeviceCmd)
}

func WriteDevice(masterBlock *[]uint64, jobNum int) {
	fileSize, _ := strconv.Atoi(InputArgs.TotalSize)
	blockSize, _ := strconv.Atoi(InputArgs.BlockSize)
	dev := pipeline.NewDevice(InputArgs.DevicePath, fileSize, blockSize, jobNum, 0, InputArgs.MasterMask, masterBlock)
	dev.WriteDevice()
}
