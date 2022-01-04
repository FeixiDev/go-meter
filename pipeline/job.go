package pipeline

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type JobRecorder struct {
	filePath string
	ch       chan [2]int
}

func NewJobRecorder(filePath string, cap int) *JobRecorder {
	// file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	ch := make(chan [2]int, cap)
	return &JobRecorder{
		filePath: filePath,
		ch:       ch,
	}
}

func (r *JobRecorder) record() {
	file, err := os.OpenFile(r.filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	defer file.Close()
	if err != nil {
		log.Fatal(err)
	}
	cap := cap(r.ch)
	for i := 0; i < cap; i++ {
		data, ok := <-r.ch
		if ok {
			line := strconv.Itoa(data[0]) + "," + strconv.Itoa(data[1]) + "\n"
			file.WriteString(line)
		}
	}
	close(r.ch)
}

func (r *JobRecorder) parse() error {
	file, err := os.OpenFile(r.filePath, os.O_RDWR, 0666)
	defer file.Close()
	if err != nil {
		log.Fatal(err)
	}
	buf := bufio.NewReader(file)
	for {
		line, err := buf.ReadString('\n')
		line = strings.Replace(line, "\n", "", -1)
		strResult := strings.Split(line, ",")
		if len(strResult) == 2 {
			start, _ := strconv.Atoi(strResult[0])
			end, _ := strconv.Atoi(strResult[1])
			r.ch <- [2]int{start, end}
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
	}
	close(r.ch)
	return nil
}

type Job struct {
	start    int
	end      int
	bs       int
	ls       int // logical size 8K
	deviceID uint64
	wg       *sync.WaitGroup
}

func Retry(timeout int, fn func() error) error {
	err := fn()
	if err != nil {
		// if s, ok := err.(stop); ok {
		// 	return s.error
		// }
		if timeout--; timeout > 0 {
			time.Sleep(1 * time.Second)
			return Retry(timeout, fn)
		}
		return err
	}
	return nil
}

func NewJob(start, end, bs int, deviceID uint64) *Job {
	return &Job{
		start:    start,
		end:      end,
		bs:       bs,
		ls:       8 * 1024,
		deviceID: deviceID,
	}
}

func (job *Job) generate(bg *BufferGroup, wg *sync.WaitGroup) {
	tempBuffer := make([]byte, 8)
	var blockID uint64
	var logicalID uint64
	var blockHeader uint64
	for i := job.start; i < job.end; i++ {
		blockID = uint64(i)
		blockHeader = job.deviceID<<48 + blockID<<32
		buffer := bg.GetFreeBuf()
		if buffer == nil {
			break
		}
		for j := 0; j < job.bs; j += job.ls {
			logicalID = uint64(j)
			dataHeader := blockHeader + logicalID
			binary.BigEndian.PutUint64(tempBuffer, dataHeader)
			for index, value := range tempBuffer {
				buffer.value[j+index] = value
			}
		}
		buffer.Ready()
	}
	wg.Done()
}

func (job *Job) Write(file *os.File, jobWg *sync.WaitGroup, ch chan [2]int) error {
	var i int
	bg := NewBufferGroup(job.bs, 2)
	wg := sync.WaitGroup{}
	wg.Add(1)

	go job.generate(bg, &wg)

	for i = job.start; i < job.end; i++ {
		buffer := bg.GetReadyBuf()
		_, err := file.WriteAt(buffer.value, int64(i*job.bs))
		if i == 100 {
			err = errors.New("人造")
		}
		if err != nil {
			fn := func() error {
				fmt.Println("重试")
				_, err := file.WriteAt(buffer.value, int64(i*job.bs))
				return err
			}
			err = Retry(30, fn)
			err = errors.New("人造")
			if err != nil {
				close(bg.stopCh)
				break
			}
		}
		buffer.Free()
	}
	ch <- [2]int{job.start, i}
	wg.Wait()
	jobWg.Done()
	return nil
}

func (job *Job) read(file *os.File) {
	block := make([]byte, job.bs)
	for i := job.start; i < job.end; i++ {
		_, err := file.ReadAt(block, int64(i*job.bs))
		if err != nil && err != io.EOF {
			panic(err)
		}
	}
}

func (job *Job) Compare(file *os.File, jobWg *sync.WaitGroup, ch chan [2]int) error {
	startAndEnd := <-ch
	jobNew := NewJob(startAndEnd[0], startAndEnd[1], job.bs, job.deviceID)
	block := make([]byte, job.bs)
	bg := NewBufferGroup(job.bs, 2)
	defer bg.Close()

	wg := sync.WaitGroup{}
	wg.Add(1)

	go jobNew.generate(bg, &wg)

	for i := jobNew.start; i < jobNew.end; i++ {
		buffer := bg.GetReadyBuf()
		_, err := file.ReadAt(block, int64(i*jobNew.bs))
		if !bytes.Equal(block, buffer.value) {
			log.Fatal("数据不一致")
		}
		if err != nil && err != io.EOF {
			return err
		}
		buffer.Free()
	}
	wg.Wait()
	jobWg.Done()
	return nil
}
