package pipeline

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"go-meter/performinfo"
	"go-meter/randnum"
	"io"
	"log"
	"os"
	"sync"
	"syscall"
	"time"
)

type File struct {
	file       *os.File
	fileSize   int
	masterMask uint64
}

func NewFileForWrite(filePath string, fileSize int, masterMask uint64) *File {
	file, err := os.OpenFile(filePath, os.O_RDWR|syscall.O_NONBLOCK|syscall.O_DIRECT|os.O_CREATE|os.O_TRUNC, 0666)
	// file, err := os.Create(filePath)
	if err != nil {
		log.Fatal(err)
	}
	return &File{
		file,
		fileSize,
		masterMask,
	}
}

func NewFileForRead(filePath string, masterMask uint64) *File {
	f, _ := os.Stat(filePath)
	// file, err := syscall.Open(filePath, syscall.O_CREAT|syscall.O_RDWR|syscall.O_NONBLOCK, 0644)
	file, err := os.OpenFile(filePath, os.O_RDWR|syscall.O_NONBLOCK, 0666)
	fileSize := int(f.Size())
	if err != nil {
		log.Fatal(err)
	}
	return &File{
		file,
		fileSize,
		masterMask,
	}
}

func MasterMap(blockID, blockSize int) int {
	fileOffset := blockID * blockSize
	masterOffset := fileOffset % MasterBlockSize
	return masterOffset
}

func (f *File) WriteFile(master *[]uint64, bs int, fileID uint64) {
	start := time.Now()
	var buffers [2][]byte
	nblocks := f.fileSize / bs
	rs := randnum.RandomInit(fileID)
	fileMask := randnum.LCGRandom(rs)
	blockMask := randnum.LCGRandom(rs)
	mask := f.masterMask ^ fileMask
	buffer1 := make([]byte, bs)
	buffer2 := make([]byte, bs)
	tempBuffer := make([]byte, 8)

	buffers[0] = buffer1
	buffers[1] = buffer2

	freeCh := make(chan int, 2)
	freeCh <- 0
	freeCh <- 1
	defer close(freeCh)

	readyCh := make(chan int, 2)
	defer close(readyCh)

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		for i := 0; i < nblocks; i++ {
			bufferID := <-freeCh
			myBuffer := buffers[bufferID]
			masterOffset := MasterMap(i, bs)
			for j := 0; j < bs; j += 8 {
				if masterOffset+j >= MasterBlockSize {
					masterOffset -= MasterBlockSize
					blockMask = randnum.LCGRandom(rs)
				}

				if masterOffset+j == 0 {
					blockMask = randnum.LCGRandom(rs)
				}

				binary.BigEndian.PutUint64(tempBuffer, (*master)[(masterOffset+j)/8]^mask^blockMask)
				for index, value := range tempBuffer {
					myBuffer[j+index] = value
				}
			}
			readyCh <- bufferID
		}
		wg.Done()
	}()

	go func() {
		// var ioID int64
		for i := 0; i < nblocks; i++ {
			// ioID = (int64(fileID)<<60 | int64(i))
			bufferID := <-readyCh
			myBuffer := buffers[bufferID]
			// performinfo.IOStart(ioID)
			f.file.Write(myBuffer)
			// performinfo.IOEnd(int64(bs), ioID)
			freeCh <- bufferID
		}
		wg.Done()
	}()
	wg.Wait()
	err := f.file.Close()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(time.Since(start))
}

func (f *File) ReadFile(bs int, fileID uint64) {
	block := make([]byte, bs)
	nblocks := f.fileSize / bs
	for i := 0; i < nblocks; i++ {
		ioID := int64(fileID) ^ int64(i)
		performinfo.IOStart(ioID)
		_, err := f.file.Read(block)
		performinfo.IOEnd(int64(bs), ioID)
		if err != nil && err != io.EOF {
			panic(err)
		}
	}
}

func (f *File) CompareFile(master *[]uint64, bs int, fileID uint64) {
	var buffers [2][]byte
	nblocks := f.fileSize / bs
	rs := randnum.RandomInit(fileID)
	fileMask := randnum.LCGRandom(rs)
	blockMask := randnum.LCGRandom(rs)
	mask := f.masterMask ^ fileMask
	buffer1 := make([]byte, bs)
	buffer2 := make([]byte, bs)
	tempBuffer := make([]byte, 8)

	buffers[0] = buffer1
	buffers[1] = buffer2

	freeCh := make(chan int, 2)
	freeCh <- 0
	freeCh <- 1
	defer close(freeCh)

	readyCh := make(chan int, 2)
	defer close(readyCh)

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		for i := 0; i < nblocks; i++ {
			bufferID := <-freeCh
			myBuffer := buffers[bufferID]
			masterOffset := MasterMap(i, bs)
			for j := 0; j < bs; j += 8 {
				if masterOffset+j >= MasterBlockSize {
					masterOffset -= MasterBlockSize
					blockMask = randnum.LCGRandom(rs)
				}
				binary.BigEndian.PutUint64(tempBuffer, (*master)[(masterOffset+j)/8]^mask^blockMask)
				for index, value := range tempBuffer {
					myBuffer[j+index] = value
				}
			}
			readyCh <- bufferID
		}
		wg.Done()
	}()

	go func() {
		block := make([]byte, bs)
		for i := 0; i < nblocks; i++ {
			bufferID := <-readyCh
			myBuffer := buffers[bufferID]
			_, err := f.file.Read(block)
			if !bytes.Equal(block, myBuffer) {
				log.Fatal("数据不一致")
			}
			if err != nil && err != io.EOF {
				panic(err)
			}
			freeCh <- bufferID
		}
		wg.Done()
	}()
	wg.Wait()
	err := f.file.Close()
	if err != nil {
		log.Fatal(err)
	}
}
