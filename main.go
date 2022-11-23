/*
command for inatall cobra
export GO111MODULE=on & export GOPROXY=https://goproxy.cn
go mod init go-meter & go run go.mod
go get github.com/spf13/cobra
go get github.com/spf13/viper
go get github.com/robfig/cron/v3
*/
package main

import (
	"go-meter/cmd"
)

func main() {
	cmd.Execute()
}
