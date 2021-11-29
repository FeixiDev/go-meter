package pipeline

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"go-meter/randnum"
	"io"
	"log"
	"math"
	"os"
	"sync"
	"time"
)

// type stop struct {
// 	error
// }

type Job struct {
	start       int
	end         int
	bs          int
	fileMask    uint64
	masterMask  uint64
	rs          *randnum.RandomState
	masterBlock *[]uint64
	wg          *sync.WaitGroup
}

func Retry(timeout int, fn func() (int, error)) (int, error) {
	n, err := fn()
	if err != nil {
		// if s, ok := err.(stop); ok {
		// 	return s.error
		// }
		if timeout--; timeout > 0 {
			time.Sleep(1 * time.Second)
			return Retry(timeout, fn)
		}
		return n, err
	}
	return n, nil
}

func GetRandomStateAndFileMask(index int, seed uint64) (*randnum.RandomState, uint64) {
	rs := randnum.RandomInit(seed)
	fileMask := randnum.LCGRandom(rs)
	for i := 0; i < index; i++ {
		randnum.LCGRandom(rs)
	}
	return rs, fileMask
}

func NewJob(start, end, bs int, fileSeed, masterMask uint64, masterBlock *[]uint64) *Job {
	index := int(math.Ceil(float64(start*bs) / float64(MasterBlockSize)))
	rs, fileMask := GetRandomStateAndFileMask(index, fileSeed)
	return &Job{
		start:       start,
		end:         end,
		bs:          bs,
		fileMask:    fileMask,
		masterMask:  masterMask,
		rs:          rs,
		masterBlock: masterBlock,
	}
}

func (job *Job) generate(bg *BufferGroup, wg *sync.WaitGroup) {
	tempBuffer := make([]byte, 8)
	blockMask := randnum.LCGRandom(job.rs)
	mask := job.masterMask ^ job.fileMask
	for i := job.start; i < job.end; i++ {
		buffer := bg.GetFreeBuf()
		masterOffset := MasterMap(i, job.bs)
		for j := 0; j < job.bs; j += 8 {
			if masterOffset+j >= MasterBlockSize {
				masterOffset -= MasterBlockSize
				blockMask = randnum.LCGRandom(job.rs)
			}
			if masterOffset+j == 0 {
				blockMask = randnum.LCGRandom(job.rs)
			}
			binary.BigEndian.PutUint64(tempBuffer, (*job.masterBlock)[(masterOffset+j)/8]^mask^blockMask)
			for index, value := range tempBuffer {
				buffer.value[j+index] = value
			}
		}
		buffer.Ready()
	}
	wg.Done()
}

func (job *Job) Write(file *os.File, jobWg *sync.WaitGroup) (int, error) {
	bg := NewBufferGroup(job.bs, 2)
	defer bg.Close()

	wg := sync.WaitGroup{}
	wg.Add(1)

	go job.generate(bg, &wg)

	for i := job.start; i < job.end; i++ {
		buffer := bg.GetReadyBuf()
		_, err := file.WriteAt(buffer.value, int64(i*job.bs))
		if err != nil {
			fn := func() (int, error) {
				_, err := file.WriteAt(buffer.value, int64(i*job.bs))
				return i, err
			}
			return Retry(30, fn)
		}
		buffer.Free()
	}
	wg.Wait()
	jobWg.Done()
	return job.end, nil
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

func (job *Job) Copy(file *os.File, jobWg *sync.WaitGroup, ch chan int) {
	n, _ := job.Write(file, jobWg)
	ch <- n
}

func (job *Job) Compare(file *os.File, jobWg *sync.WaitGroup, ch chan int) error {
	fmt.Println(".")
	n := <-ch
	jobNew := NewJob(job.start, n, job.bs, 0, job.masterMask, job.masterBlock)
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
