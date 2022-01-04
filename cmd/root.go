package cmd

import (
	"fmt"
	"go-meter/performinfo"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/gosuri/uitable"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var InputArgs struct {
	Lineage    []uint
	JobNum     int
	BlockSize  string
	TotalSize  string
	MasterMask uint64
	FilePath   string
	DevicePath map[string]uint64
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "go-meter",
	Short: "Tool of copy and compare",
	Long:  `go-meter is a tool of copy and compare.`,
	Args:  cobra.ExactArgs(1),
	// Run: func(cmd *cobra.Command, args []string) {

	// },
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	cobra.CheckErr(rootCmd.Execute())
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().UintSliceVarP(&InputArgs.Lineage, "lineage", "l", nil, "Start Lineage,End Lineage")
	rootCmd.PersistentFlags().IntVarP(&InputArgs.JobNum, "job", "j", 0, "Job Num")
	rootCmd.PersistentFlags().StringVarP(&InputArgs.TotalSize, "tsize", "t", "", "Total Size")
	rootCmd.PersistentFlags().StringVarP(&InputArgs.BlockSize, "bsize", "b", "", "Block Size")
	rootCmd.PersistentFlags().StringVarP(&InputArgs.FilePath, "path", "p", "", "FilePath")
	// rootCmd.PersistentFlags().StringSliceVarP(&InputArgs.DevicePath, "device", "d", []string{}, "DevicePath")
	rootCmd.PersistentFlags().Uint64VarP(&InputArgs.MasterMask, "mask", "m", 0, "Master Mask")

	viper.BindPFlag("TotalSize", rootCmd.PersistentFlags().Lookup("tsize"))
	viper.BindPFlag("JobNum", rootCmd.PersistentFlags().Lookup("job"))
	viper.BindPFlag("BlockSize", rootCmd.PersistentFlags().Lookup("bsize"))
	viper.BindPFlag("MasterMask", rootCmd.PersistentFlags().Lookup("mask"))
	viper.BindPFlag("FilePath", rootCmd.PersistentFlags().Lookup("path"))
	viper.BindPFlag("DevicePath", rootCmd.PersistentFlags().Lookup("device"))
	viper.BindPFlag("Lineage", rootCmd.PersistentFlags().Lookup("lineage"))
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	viper.AddConfigPath(".")
	viper.SetConfigName("go-meter")
	viper.SetConfigType("yaml")
	// viper.SetConfigFile("go-meter.yaml")

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	var err error
	if err = viper.ReadInConfig(); err == nil {
		fmt.Fprintln(os.Stderr, "Using config file:", viper.ConfigFileUsed())
	}

	// if _, ok := err.(viper.ConfigFileNotFoundError); ok {
	// 	// Config file not found; ignore error if desired
	// 	fmt.Println("no such config file")
	// } else {
	// 	// Config file was found but another error was produced
	// 	fmt.Println("read config error")
	// }

	// read config from yaml file
	viper.UnmarshalKey("TotalSize", &InputArgs.TotalSize)
	viper.UnmarshalKey("JobNum", &InputArgs.JobNum)
	viper.UnmarshalKey("BlockSize", &InputArgs.BlockSize)
	viper.UnmarshalKey("MasterMask", &InputArgs.MasterMask)
	viper.UnmarshalKey("FilePath", &InputArgs.FilePath)
	viper.UnmarshalKey("DevicePath", &InputArgs.DevicePath)
	viper.UnmarshalKey("Lineage", &InputArgs.Lineage)

	// fmt.Println("Total Size:", viper.GetString("TotalSize"))

}

//Show performance with table style
func printPerfor() {
	table := uitable.New()
	table.Separator = "\t\t"
	table.AddRow("IOPS", "MBPS", "CPU Utilization", "Memory Utilization")
	table.AddRow(performinfo.GetIOps(), performinfo.GetMBps(), changeFtoS(performinfo.GetState()[0]), changeFtoS(performinfo.GetState()[1]))
	// fmt.Println("IOPS:", performinfo.GetIOps())
	// fmt.Println("MBPS:", performinfo.GetMBps())
	// fmt.Println("CPU:", performinfo.GetState())
	fmt.Println(table)
	fmt.Println()
}

// Check the format of size
func checkSize(size string, sizetype string) string {
	strSize := strings.ToUpper(size)
	str := `^([0-9.]+)(K|M|G|T)(?:I?B)?$`
	r := regexp.MustCompile(str)
	matchsResult := r.FindStringSubmatch(strSize)
	if len(matchsResult) == 0 {
		fmt.Println("Please input correct size of", sizetype)
		os.Exit(1)
	}
	finalSize, _ := strconv.Atoi(matchsResult[1])
	switch matchsResult[2] {
	case "K":
		finalSize = finalSize * 1024
	case "M":
		finalSize = finalSize * 1024 * 1024
	case "G":
		finalSize = finalSize * 1024 * 1024 * 1024
	case "T":
		finalSize = finalSize * 1024 * 1024 * 1024 * 1024
	}
	return strconv.Itoa(finalSize)
}

//Check the format of input args and change the unit of size to byte
func checkInputArgs() {
	InputArgs.BlockSize = checkSize(InputArgs.BlockSize, "Block")
	InputArgs.TotalSize = checkSize(InputArgs.TotalSize, "Total File")
	if len(InputArgs.Lineage) > 2 || len(InputArgs.Lineage) == 1 {
		fmt.Println("Please input correct Start Lineage and End Lineage.")
		os.Exit(1)
	}
	if InputArgs.Lineage[0] > InputArgs.Lineage[1] {
		fmt.Println("Start Lineage cannot be greater than End Lineage.")
		os.Exit(1)
	}
}

func checkBlockArgs() {
	InputArgs.BlockSize = checkSize(InputArgs.BlockSize, "Block")
	InputArgs.TotalSize = checkSize(InputArgs.TotalSize, "Total File")
	if InputArgs.JobNum == 0 {
		fmt.Println("Please input correct number of job (JobNum).")
		os.Exit(1)
	}
	blockSize, _ := strconv.Atoi(InputArgs.BlockSize)
	if blockSize%8192 != 0 {
		fmt.Println("The block size must be an integer multiple of 8K")
		os.Exit(1)
	}

}

func checkCompareCondition() {
	InputArgs.BlockSize = checkSize(InputArgs.BlockSize, "Block")
	InputArgs.TotalSize = checkSize(InputArgs.TotalSize, "Total File")
	if InputArgs.JobNum == 0 {
		fmt.Println("Please input correct number of job (JobNum).")
		os.Exit(1)
	}
}

//Change float64 to '%' type string
func changeFtoS(value float64) string {
	result := strconv.FormatFloat(value, 'f', 2, 64)
	return result + "%"
}
