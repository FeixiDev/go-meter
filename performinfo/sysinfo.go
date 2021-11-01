package performinfo

import (
    "sync"
    "time"
    "strconv"
    "github.com/shirou/gopsutil/cpu"
    "github.com/shirou/gopsutil/mem"
)

const (
	ioWindow int64 = 3
)

type IOState struct {
	IOPS map[int64]float64
	MBPS map[int64]float64
	IOMutex sync.Mutex
}

type IOInfo struct {
	IOStime map[int64]int64
	IOMutex sync.Mutex
}

var ioinfo = IOInfo{
	IOStime: make(map[int64]int64),
}
var iostate = IOState{
	IOPS: make(map[int64]float64),
	MBPS: make(map[int64]float64),
}

func GetState()([]float64){

    sysInfo := make([]float64,2)
    cpuPer, _ := cpu.Percent(time.Second, false)
    sysInfo[0] = cpuPer[0]
    memInfo,_ := mem.VirtualMemory()
    sysInfo[1] = memInfo.UsedPercent

    return sysInfo

}

func IOStart(ioid int64) error {
	stime := time.Now().Unix()
	ioinfo.IOMutex.Lock()
	
	ioinfo.IOStime[ioid] = stime
	ioinfo.IOMutex.Unlock()
	return nil
}

func IOEnd(bs int64, ioid int64) error {
	var i int64
	etime := time.Now().Unix()
	ioinfo.IOMutex.Lock()
	iostate.IOMutex.Lock()

	ioetime := etime
	for id := range ioinfo.IOStime{
		if ioid == id {
			if ioetime - ioinfo.IOStime[ioid] > ioWindow {
				delete(ioinfo.IOStime, ioid)
				break
			} 
			cycles := ioWindow + 1 - ioetime + ioinfo.IOStime[ioid]
			for i = 0; i < cycles; i++ {
				iostate.IOPS[ioetime + i] += 1 / float64(ioetime - ioinfo.IOStime[ioid] + i)
				iostate.MBPS[ioetime + i] += float64(bs) / float64(ioetime - ioinfo.IOStime[ioid] + i)
			} 
		}
	}
	delete(ioinfo.IOStime, ioid)
	ioinfo.IOMutex.Unlock()
	iostate.IOMutex.Unlock()
	return nil
}

func GetIOps() (float64) {
	nowtime := time.Now().Unix()
	gettime := nowtime - 1

	iops, ok:= iostate.IOPS[gettime]
	if ok {
		value, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", iops), 64)
		delete(iostate.IOPS, gettime)
		return value
	} else {
		return 0
	}
}

func GetMBps() (float64) {
	nowtime := time.Now().Unix()
	gettime := nowtime - 1

	mbps, ok:= iostate.MBPS[gettime]
	if ok {
		value, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", (mbps / (1024 * 1024))), 64)
		delete(iostate.MBPS, gettime)
		return value
	} else {
		return 0
	}
}
