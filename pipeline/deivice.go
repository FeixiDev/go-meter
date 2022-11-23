package pipeline

import (
	"log"
	"os"
	"sync"
	"syscall"
)

type Device struct {
	path    string
	id      uint64
	jobList []*Job
}

func NewDevice(devicePath string, deviceID uint64, size, bs, jobNum int) *Device {
	blockNum := size / bs
	amount := blockNum / jobNum
	remainder := blockNum % jobNum
	jobList := make([]*Job, jobNum)

	for i := 0; i < jobNum; i++ {
		job := NewJob(i*amount, (i+1)*amount, bs, deviceID)
		jobList[i] = job
	}

	if remainder != 0 {
		job := NewJob(jobNum*amount, jobNum*amount+remainder, bs, deviceID)
		jobList = append(jobList, job)
	}

	return &Device{
		path:    devicePath,
		id:      deviceID,
		jobList: jobList,
	}
}

func (d *Device) WriteDevice() {
	// dev, err := os.OpenFile(d.path, syscall.O_RDWR|syscall.O_NONBLOCK|syscall.O_DIRECT, 0666)
	dev, err := os.OpenFile(d.path, os.O_RDWR|syscall.O_NONBLOCK|syscall.O_DIRECT|os.O_CREATE|os.O_TRUNC, 0666)
	defer dev.Close()
	if err != nil {
		log.Fatal(err)
	}
	wg := sync.WaitGroup{}
	wg.Add(len(d.jobList))
	recorder := NewJobRecorder("record.txt", len(d.jobList))
	for _, job := range d.jobList {
		go job.Write(dev, &wg, recorder.ch)
	}
	wg.Wait()
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
