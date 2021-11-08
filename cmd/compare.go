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
		checkInputArgs()
		number := InputArgs.Lineage[1] - InputArgs.Lineage[0] + 1
		wg := &sync.WaitGroup{}
		wg.Add(int(number))
		masterBlock := pipeline.MasterBlockInit()
		for i := InputArgs.Lineage[0]; i <= InputArgs.Lineage[1]; i++ {
			go CompareFiles(i, masterBlock, wg)
		}
		wg.Wait()
		fmt.Println("Finish to compare files...")
	},
}

func init() {
	rootCmd.AddCommand(compareCmd)
}

func CompareFiles(i uint, masterBlock *[]uint64, wg *sync.WaitGroup) {
	fileID := uint64(i)
	blockSize, _ := strconv.Atoi(InputArgs.BlockSize)
	fileSize, _ := strconv.Atoi(InputArgs.TotalSize)
	filename := InputArgs.FilePath + "/" + strconv.FormatUint(fileID, 10)
	file := pipeline.NewFileForRead(filename, fileSize, InputArgs.MasterMask)
	file.CompareFile(masterBlock, blockSize, fileID)
	wg.Done()
}
