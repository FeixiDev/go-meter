package pipeline

type buffer struct {
	id    int
	value []byte
	bg    *BufferGroup
}

type BufferGroup struct {
	buffers         []buffer
	freeCh, readyCh chan int
}

func NewBufferGroup(size int, num int) *BufferGroup {
	var bufferGroup BufferGroup

	buffers := make([]buffer, num)
	freeCh := make(chan int, num)
	readyCh := make(chan int, num)
	for i := 0; i < num; i++ {
		buffers[i] = buffer{i, make([]byte, size), &bufferGroup}
		freeCh <- i
	}
	bufferGroup.buffers = buffers
	bufferGroup.freeCh = freeCh
	bufferGroup.readyCh = readyCh

	return &bufferGroup
}

func (b *buffer) Free() {
	b.bg.freeCh <- b.id
}

func (b *buffer) Ready() {
	b.bg.readyCh <- b.id
}

func (bg *BufferGroup) Close() {
	close(bg.freeCh)
	close(bg.readyCh)
}

func (bg *BufferGroup) GetFreeBuf() *buffer {
	bufferID := <-bg.freeCh
	nullBuf := bg.buffers[bufferID]
	return &nullBuf
}

func (bg *BufferGroup) GetReadyBuf() *buffer {
	bufferID := <-bg.readyCh
	fullBuf := bg.buffers[bufferID]
	return &fullBuf
}
