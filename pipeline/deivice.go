package pipeline

import (
	"log"
	"os"
	"sync"
	"syscall"
)

type Device struct {
	path    *os.File
	jobList []*Job
}

func NewDevice(devicePath string, size, bs, jobNum int, fileSeed, masterMask uint64, masterBlock *[]uint64) *Device {
	// dev, err := os.OpenFile(devicePath, syscall.O_RDWR|syscall.O_NONBLOCK|syscall.O_DIRECT, 0666)
	dev, err := os.OpenFile(devicePath, os.O_RDWR|syscall.O_NONBLOCK|syscall.O_DIRECT|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatal(err)
	}

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
		path:    dev,
		jobList: jobList,
	}
}

func (dev *Device) WriteDevice() {
	wg := sync.WaitGroup{}
	wg.Add(len(dev.jobList))
	for _, job := range dev.jobList {
		// go job.write(dev.path, &wg)
		go job.Write(dev.path, &wg)
	}
	wg.Wait()
}

func (dev *Device) CompareDevice() {
	wg := sync.WaitGroup{}
	wg.Add(len(dev.jobList))
	ch := make(chan int, len(dev.jobList))
	for _, job := range dev.jobList {
		// go job.write(dev.path, &wg)
		go job.Copy(dev.path, &wg, ch)
	}
	wg.Wait()

	wg2 := sync.WaitGroup{}
	wg2.Add(len(dev.jobList))
	for _, job := range dev.jobList {
		go job.Compare(dev.path, &wg2, ch)
	}
	wg2.Wait()
}
