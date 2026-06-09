package bytes_buffer

type Buffer struct {
	buf []byte
	off int
}

func NewBuffer() *Buffer {
	return &Buffer{}
}

func NewBufferFromBytes(b []byte) *Buffer {
	return &Buffer{buf: b}
}
