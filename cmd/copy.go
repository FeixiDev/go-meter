package cmd

import (
	"fmt"
	"go-meter/pipeline"
	"strconv"
	"sync"

	"github.com/robfig/cron"
	"github.com/spf13/cobra"
)

// copyCmd represents the copy command
var copyCmd = &cobra.Command{
	Use:   "copy",
	Short: "copy",
	Long:  `Copy`,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Start to write files...")
		checkInputArgs()
		number := InputArgs.Lineage[1] - InputArgs.Lineage[0] + 1
		wg := &sync.WaitGroup{}
		wg.Add(int(number))

		c := cron.New()
		c.AddFunc("@every 1s", func() {
			printPerfor()
		})
		c.Start()

		masterBlock := pipeline.MasterBlockInit()
		for i := InputArgs.Lineage[0]; i <= InputArgs.Lineage[1]; i++ {
			go WriteFiles(i, masterBlock, wg)
		}

		wg.Wait()
		fmt.Println("Finish to write files...")
	},
}

func init() {
	rootCmd.AddCommand(copyCmd)
}

// Write file with Lineage
func WriteFiles(i uint, masterBlock *[]uint64, wg *sync.WaitGroup) {
	fileID := uint64(i)
	fileSize, _ := strconv.Atoi(InputArgs.TotalSize)
	blockSize, _ := strconv.Atoi(InputArgs.BlockSize)
	filename := InputArgs.FilePath + "/" + strconv.FormatUint(fileID, 10)
	file := pipeline.NewFileForWrite(filename, fileSize, InputArgs.MasterMask)
	file.WriteFile(masterBlock, blockSize, fileID)
	wg.Done()
}
