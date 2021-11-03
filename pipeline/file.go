package pipeline

import (
	"encoding/binary"
	"go-meter/performinfo"
	"go-meter/randnum"
	"log"
	"os"
	"sync"
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
	fileOffset := blockID * blockSize
	masterOffset := fileOffset % MasterBlockSize
	return masterOffset
}

func (f *File) WriteFile(master *[]uint64, bs int, fileID uint64) {
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
		var ioID int64
		for i := 0; i < nblocks; i++ {
			ioID = int64(fileID) ^ int64(i)
			bufferID := <-readyCh
			myBuffer := buffers[bufferID]
			performinfo.IOStart(ioID)
			f.file.Write(myBuffer)
			performinfo.IOEnd(int64(bs), ioID)
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
