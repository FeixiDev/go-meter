package pipeline

import (
	"encoding/binary"
	"fmt"
	"go-meter/performinfo"
	"go-meter/randnum"
	"log"
	"os"
	"sync"
	"time"
)

type File struct {
	file       *os.File
	fileSize   int
	masterMask uint64
}

func NewFile(filePath string, fileSize int, masterMask uint64) *File {
	file, err := os.Create(filePath)
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
	blockSize = blockSize / 8
	masterBlockSize := MasterBlockSize / 8
	fileOffset := blockID * blockSize
	masterOffset := fileOffset % masterBlockSize
	return masterOffset
}

func (f *File) WriteFile1(master *[]uint64, bs int, fileID uint64) {
	start := time.Now()
	rs := randnum.RandomInit(fileID)
	fileMask := randnum.LCGRandom(rs)
	blockMask := randnum.LCGRandom(rs)
	mask := f.masterMask ^ fileMask
	buffer := make([]byte, bs)
	tempBuffer := make([]byte, 8)

	for i := 0; i < f.fileSize/bs; i++ {
		masterOffset := MasterMap(i, bs)
		for j := 0; j < bs/8; j++ {
			if masterOffset+j == MasterBlockSize/8 {
				masterOffset = masterOffset - MasterBlockSize/8
				blockMask = randnum.LCGRandom(rs)
			}
			binary.BigEndian.PutUint64(tempBuffer, (*master)[masterOffset+j]^mask^blockMask)
			for index, value := range tempBuffer {
				buffer[j*8+index] = value
			}

		}
		f.file.Write(buffer)
		performinfo.IOEnd(int64(bs))
	}
	err := f.file.Close()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("不使用channel:", time.Since(start))
}

func (f *File) WriteFile(master *[]uint64, bs int, fileID uint64) {
	// start := time.Now()
	var buffers [2][]byte
	rs := randnum.RandomInit(fileID)
	fileMask := randnum.LCGRandom(rs)
	blockMask := randnum.LCGRandom(rs)
	mask := f.masterMask ^ fileMask
	buffer1 := make([]byte, bs)
	buffer2 := make([]byte, bs)
	tempBuffer := make([]byte, 8)

	buffers[0] = buffer1
	buffers[1] = buffer2

	writeCh := make(chan int, 2)
	writeCh <- 0
	writeCh <- 1
	defer close(writeCh)

	readyCh := make(chan int, 2)
	defer close(readyCh)

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		for i := 0; i < f.fileSize/bs; i++ {
			myChan := <-writeCh
			myBuffer := buffers[myChan]
			masterOffset := MasterMap(i, bs)
			for j := 0; j < bs/8; j++ {
				if masterOffset+j == MasterBlockSize/8 {
					masterOffset = masterOffset - MasterBlockSize/8
					blockMask = randnum.LCGRandom(rs)
				}
				binary.BigEndian.PutUint64(tempBuffer, (*master)[masterOffset+j]^mask^blockMask)
				for index, value := range tempBuffer {
					myBuffer[j*8+index] = value
				}
			}
			readyCh <- myChan
		}
		wg.Done()
	}()

	go func() {
		for i := 0; i < f.fileSize/bs; i++ {
			myChan := <-readyCh
			myBuffer := buffers[myChan]
			f.file.Write(myBuffer)
			performinfo.IOEnd(int64(bs))
			writeCh <- myChan
		}
		wg.Done()
	}()
	wg.Wait()
	err := f.file.Close()
	if err != nil {
		log.Fatal(err)
	}
	// fmt.Println("使用channel:", time.Since(start))
}
