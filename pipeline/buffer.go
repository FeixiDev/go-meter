package pipeline

type buffer struct {
	id    int
	value []byte
	bg    *BufferGroup
}

type BufferGroup struct {
	buffers         []buffer
	freeCh, readyCh chan int
	stopCh          chan struct{}
}

func NewBufferGroup(size int, num int) *BufferGroup {
	var bufferGroup BufferGroup

	buffers := make([]buffer, num)
	freeCh := make(chan int, num)
	readyCh := make(chan int, num)
	stopCh := make(chan struct{})
	for i := 0; i < num; i++ {
		buffers[i] = buffer{i, make([]byte, size), &bufferGroup}
		freeCh <- i
	}
	bufferGroup.buffers = buffers
	bufferGroup.freeCh = freeCh
	bufferGroup.readyCh = readyCh
	bufferGroup.stopCh = stopCh

	return &bufferGroup
}

func (b *buffer) Free() {
	b.bg.freeCh <- b.id
}

func (b *buffer) Ready() {
	b.bg.readyCh <- b.id
}

func (b *buffer) Stop() {
	b.bg.freeCh <- -1
}

func (bg *BufferGroup) Close() {
	close(bg.freeCh)
	close(bg.readyCh)
}

func (bg *BufferGroup) GetFreeBuf() *buffer {
	select {
	case <-bg.stopCh:
		bg.Close()
		return nil
	case bufferID := <-bg.freeCh:
		nullBuf := bg.buffers[bufferID]
		return &nullBuf
	}

	// bufferID := <-bg.freeCh
	// nullBuf := bg.buffers[bufferID]
	// return &nullBuf
}

func (bg *BufferGroup) GetReadyBuf() *buffer {
	bufferID := <-bg.readyCh
	fullBuf := bg.buffers[bufferID]
	return &fullBuf
}
