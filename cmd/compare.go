package cmd

import (
	"fmt"
	"go-meter/pipeline"
	"strconv"
	"sync"

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
		checkBlockArgs()
		jobNum := InputArgs.JobNum
		deviceMap := InputArgs.DevicePath
		// masterBlock := pipeline.MasterBlockInit()
		wg := &sync.WaitGroup{}
		wg.Add(len(deviceMap))
		for device, deviceID := range deviceMap {
			CompareFiles(device, deviceID, jobNum, wg)
		}
		wg.Wait()
		fmt.Println("Finish to compare files...")
	},
}

func init() {
	rootCmd.AddCommand(compareCmd)
}

func CompareFiles(device string, deviceID uint64, jobNum int, wg *sync.WaitGroup) {
	fileSize, _ := strconv.Atoi(InputArgs.TotalSize)
	blockSize, _ := strconv.Atoi(InputArgs.BlockSize)

	dev := pipeline.NewDevice(device, deviceID, fileSize, blockSize, jobNum)
	dev.CompareDevice()
	wg.Done()
}
