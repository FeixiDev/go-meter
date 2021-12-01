package pipeline

import (
	"log"
	"os"
	"sync"
	"syscall"
)

type Device struct {
	path    string
	jobList []*Job
}

func NewDevice(devicePath string, size, bs, jobNum int, fileSeed, masterMask uint64, masterBlock *[]uint64) *Device {
	// dev, err := os.OpenFile(devicePath, syscall.O_RDWR|syscall.O_NONBLOCK|syscall.O_DIRECT, 0666)
	// dev, err := os.OpenFile(devicePath, os.O_RDWR|syscall.O_NONBLOCK|syscall.O_DIRECT|os.O_CREATE, 0666)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	blockNum := size / bs
	amount := blockNum / jobNum
	remainder := blockNum % jobNum
	jobList := make([]*Job, jobNum)

	for i := 0; i < jobNum; i++ {
		job := NewJob(i*amount, (i+1)*amount, bs, fileSeed, masterMask, masterBlock)
		jobList[i] = job
	}

	if remainder != 0 {
		job := NewJob(jobNum*amount, jobNum*amount+remainder, bs, fileSeed, masterMask, masterBlock)
		jobList = append(jobList, job)
	}

	return &Device{
		path:    devicePath,
		jobList: jobList,
	}
}

func (d *Device) WriteDevice() {
	dev, err := os.OpenFile(d.path, syscall.O_RDWR|syscall.O_NONBLOCK|syscall.O_DIRECT, 0666)
	// dev, err := os.OpenFile(d.path, os.O_RDWR|syscall.O_NONBLOCK|syscall.O_DIRECT|os.O_CREATE|os.O_TRUNC, 0666)
	defer dev.Close()
	if err != nil {
		log.Fatal(err)
	}
	wg := sync.WaitGroup{}
	wg.Add(len(d.jobList))
	recorder := NewJobRecorder("record.txt", len(d.jobList))
	for _, job := range d.jobList {
		// go job.write(dev.path, &wg)
		go job.Write(dev, &wg, recorder.ch)
	}
	wg.Wait()
	// 已结束写入，进行部分数据的记录
	recorder.record()
}

func (d *Device) CompareDevice() {
	dev, err := os.OpenFile(d.path, os.O_RDWR|syscall.O_NONBLOCK|syscall.O_DIRECT, 0666)
	defer dev.Close()
	if err != nil {
		log.Fatal(err)
	}
	wg := sync.WaitGroup{}
	wg.Add(len(d.jobList))

	recorder := NewJobRecorder("record.txt", len(d.jobList))
	errRecord := recorder.parse()
	if errRecord != nil {
		log.Fatal(errRecord)
	}
	for _, job := range d.jobList {
		go job.Compare(dev, &wg, recorder.ch)
	}
	wg.Wait()

}
