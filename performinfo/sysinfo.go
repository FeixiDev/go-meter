package performinfo

import (
    "fmt"
    "time"
    "strconv"
    "sync"
    "github.com/shirou/gopsutil/cpu"
    "github.com/shirou/gopsutil/mem"
)

const (
    ioWindow int64 = 3
)

type IOInfo struct {
    // IOStime map[int64]int64
    // IOEtime map[int64]int64
    IObs int64
    IOMutex sync.RWMutex
}

// var ioinfo = IOInfo {
//     IOStime: make(map[int64]int64),
//     IOEtime: make(map[int64]int64),
// }

var ioinfo IOInfo
var IOCount = make(map[int64]int64)

func GetState()([]float64) {
    sysInfo := make([]float64, 2)
    cpuPer, _ := cpu.Percent(time.Second, false)
    sysInfo[0] = cpuPer[0]
    memInfo, _ := mem.VirtualMemory()
    sysInfo[1] = memInfo.UsedPercent

    return sysInfo

}

/*IOStart 目前没有实际的操作*/
func IOStart(ioid int64) error {
    // stime := time.Now().Unix()
    // ioinfo.IOMutex.Lock()
    // ioinfo.IOStime[ioid] = stime
    // ioinfo.IOMutex.Unlock()
    return nil
}

/*IOEnd 统计每一秒的IO个数*/
func IOEnd(bs int64, ioid int64) error {
    ioinfo.IOMutex.Lock()
    if ioinfo.IObs == 0 {
        ioinfo.IObs = bs
    }
    etime := time.Now().Unix()
    // ioinfo.IOEtime[ioid] = etime
    IOCount[etime] += 1
    ioinfo.IOMutex.Unlock()
    return nil
}

/*获取IOPS*/
func GetIOps() (float64) {
    var i int64
    var iops float64
    nowtime := time.Now().Unix()
    gettime := nowtime - 1
    ioinfo.IOMutex.Lock()
    for i = 0; i < ioWindow; i++ {
        iops += float64(IOCount[gettime - i])
    }
    ioinfo.IOMutex.Unlock()
    value, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", iops / float64(ioWindow)), 64)
    return value
}

/*获取MBPS*/
func GetMBps() (float64) {
    var i int64
    var mbps float64
    nowtime := time.Now().Unix()
    gettime := nowtime - 1
    ioinfo.IOMutex.Lock()
    for i = 0; i < ioWindow; i++ {
        mbps += float64(ioinfo.IObs) * float64(IOCount[gettime - i])
    }
    delete(IOCount, gettime - ioWindow)
    ioinfo.IOMutex.Unlock()
    value, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", mbps / float64(ioWindow * 1024 * 1024)), 64)
    return value
}
