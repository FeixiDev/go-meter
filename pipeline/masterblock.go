package pipeline

import (
	"go-meter/randnum"
)

const MasterBlockSize = 64 * 1024
const RandomTimes = 8 * 1024

// 初始化 MasterBlock 使用的Seed是1
func MasterBlockInit() *[]uint64 {
	rs := randnum.RandomInit(1)
	masterBlock := make([]uint64, RandomTimes)
	for i := 0; i < RandomTimes; i++ {
		section := randnum.LCGRandom(rs)
		masterBlock[i] = section
	}
	return &masterBlock
}
