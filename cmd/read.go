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
var readCmd = &cobra.Command{
	Use:   "read",
	Short: "read",
	Long:  `Read`,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Start to read files...")
		checkInputArgs()
		number := InputArgs.Lineage[1] - InputArgs.Lineage[0] + 1
		wg := &sync.WaitGroup{}
		wg.Add(int(number))

		c := cron.New()
		c.AddFunc("@every 1s", func() {
			printPerfor()
		})
		c.Start()

		// masterBlock := pipeline.MasterBlockInit()
		for i := InputArgs.Lineage[0]; i <= InputArgs.Lineage[1]; i++ {
			go ReadFiles(i, wg)
		}
		wg.Wait()
		fmt.Println("Finish to read files...")
	},
}

func init() {
	rootCmd.AddCommand(readCmd)
}

// Write file with Lineage
func ReadFiles(i uint, wg *sync.WaitGroup) {
	fileID := uint64(i)
	blockSize, _ := strconv.Atoi(InputArgs.BlockSize)
	filename := InputArgs.FilePath + "/" + strconv.FormatUint(fileID, 10)
	// f, err := os.Stat(filename)
	// if err != nil {
	// 	log.Fatal("File(s) does not exist")
	// }
	file := pipeline.NewFileForRead(filename, InputArgs.MasterMask)
	file.ReadFile(blockSize, fileID)
	wg.Done()
}
