package cmd

import (
	"fmt"
	"go-meter/pipeline"
	"strconv"
	"sync"

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

		jobNum := InputArgs.JobNum
		deviceMap := InputArgs.DevicePath
		wg := &sync.WaitGroup{}
		wg.Add(len(deviceMap))
		for device, deviceID := range deviceMap {
			go WriteDevice(device, deviceID, jobNum, wg)
		}
		wg.Wait()
		fmt.Println("Finish to write files...")
	},
}

func init() {
	rootCmd.AddCommand(WriteDeviceCmd)
}

func WriteDevice(device string, deviceID uint64, jobNum int, wg *sync.WaitGroup) {
	fileSize, _ := strconv.Atoi(InputArgs.TotalSize)
	blockSize, _ := strconv.Atoi(InputArgs.BlockSize)
	dev := pipeline.NewDevice(device, deviceID, fileSize, blockSize, jobNum)
	dev.WriteDevice()
	wg.Done()
}
