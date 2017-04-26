// package buffer provides an mmap'd circular buffer
package buffer

import (
	"errors"
	"os"
	"sync"
	"syscall"

	"github.com/cloudflare/buffer/binary"
)

// Buffer data format
// Total size doesn't include header
// (32 bytes)
// +----------------+
// |   total size   |
// +----------------+
// | next read off  |
// +----------------+
// | next write off |
// +----------------+
// |    capacity    |
// +----------------+
// |      data      |
// |      ...       |
// +----------------+

type Buffer struct {
	sync.Mutex
	data []byte
}

type meta struct {
	size uint64
	woff uint64
	roff uint64
	cap  uint64
}

// TODO: add magic + signatures
const (
	offSize        = iota * 8
	offNextRead    = iota * 8
	offNextWrite   = iota * 8
	offMaxCapacity = iota * 8
	offData        = iota * 8
)

// recordLength contains metadata about what's stored.
// Currently only the recordLength
const recMeta = 8

func (b *Buffer) readmeta() meta {
	return meta{
		size: binary.GetLittleEndianUint64(b.data, offSize),
		roff: binary.GetLittleEndianUint64(b.data, offNextRead),
		woff: binary.GetLittleEndianUint64(b.data, offNextWrite),
		cap:  binary.GetLittleEndianUint64(b.data, offMaxCapacity),
	}
}

func (b *Buffer) writemeta(m meta) {
	binary.PutLittleEndianUint64(b.data, offSize, m.size)
	binary.PutLittleEndianUint64(b.data, offNextRead, m.roff)
	binary.PutLittleEndianUint64(b.data, offNextWrite, m.woff)
	binary.PutLittleEndianUint64(b.data, offMaxCapacity, m.cap)
}

var (
	errToBig     = errors.New("write exceeds file capacity")
	errOverwrite = errors.New("write would overwrite another record")
	errOverflow  = errors.New("write exceeds remaining available space")
)

// Insert inserts the data into the buffer
// Inserting data which will overflow the buffer fails
// Inserting does not overwrite any existing data inside the buffer
// if it would overwrite data, errOverwrite is returned
func (b *Buffer) Insert(data []byte) error {
	b.Lock()
	defer b.Unlock()

	m := b.readmeta()
	writeLen := uint64(len(data))

	// data is larger filesize - metadata
	if writeLen > (m.cap - offData - recMeta) {
		return errToBig
	}

	// data exceeds available space
	if m.size+writeLen+offData+recMeta > m.cap {
		return errOverflow
	}

	wrap := false
	copyTo := 0
	endOff := m.woff + writeLen + recMeta
	if endOff >= m.cap {
		// Get to the start of the file
		endOff = endOff % m.cap
		// save for later
		copyTo = int(writeLen - endOff)
		// skip the metadata
		endOff = endOff + offData
		wrap = true
	}

	// TODO(alan): is it possible to hit _any_ of these if we already check Overflow
	switch {
	//         r   e   w
	// [_______++++++++____]
	case m.roff < m.woff && m.roff < endOff && endOff < m.woff:
		return errOverwrite
	//         w       r e
	// [+++++++________++++]
	case m.woff < m.roff && endOff > m.roff:
		return errOverwrite
	//     e   w       r
	// [+++++++________++++]
	case m.woff < m.roff && endOff < m.woff:
		return errOverwrite
	}

	switch {
	case wrap && m.woff+recMeta > m.cap:
		tmp := serializeMeta(writeLen)
		left := m.cap - m.woff
		// startData := (m.woff+recMeta)%m.cap + offData
		copy(b.data[m.woff:m.cap], tmp[:left])
		copy(b.data[offData:offData+recMeta-left], tmp[left:])
		copy(b.data[offData+recMeta-left:endOff], data)
	case wrap:
		binary.PutLittleEndianUint64(b.data, int(m.woff), uint64(writeLen))
		copy(b.data[m.woff+recMeta:m.cap], data[:copyTo])
		copy(b.data[offData:endOff], data[copyTo:])
	default:
		// write doesn't wrap
		binary.PutLittleEndianUint64(b.data, int(m.woff), uint64(writeLen))
		copy(b.data[m.woff+recMeta:endOff], data)
	}
	m.woff = endOff
	m.size += (writeLen + recMeta)
	b.writemeta(m)
	return nil
}

// serializeMeta returns the serialized form of the record
func serializeMeta(recLen uint64) []byte {
	ret := make([]byte, recMeta)
	binary.PutLittleEndianUint64(ret, 0, recLen)
	return ret
}

func deserializeMeta(meta []byte) uint64 {
	return binary.GetLittleEndianUint64(meta, 0)
}

func (b *Buffer) read(mutate bool) ([]byte, error) {
	b.Lock()
	defer b.Unlock()

	m := b.readmeta()

	if m.size == 0 {
		return nil, nil
	}

	var (
		readLen uint64
		left    uint64
	)
	if m.roff+recMeta > m.cap {
		left = m.cap - m.roff
		tmp := make([]byte, recMeta)
		copy(tmp[:left], b.data[m.roff:m.cap])
		copy(tmp[left:], b.data[offData:offData+recMeta-left])
		readLen = deserializeMeta(tmp)
	} else {
		readLen = binary.GetLittleEndianUint64(b.data, int(m.roff))
	}

	var copyTo int
	endOff := m.roff + readLen + recMeta
	wrap := false
	if endOff > m.cap {
		// Get to the start of the file
		endOff = endOff % m.cap
		// save for later
		copyTo = int(readLen - endOff)
		// skip the metadata
		endOff = endOff + offData
		wrap = true
	}

	ret := make([]byte, readLen)

	switch {
	case wrap && m.roff+recMeta > m.cap:
		copy(ret, b.data[offData+recMeta-left:endOff])
	case wrap:
		copy(ret[:copyTo], b.data[m.roff+recMeta:m.cap])
		copy(ret[copyTo:], b.data[offData:endOff])
	default:
		copy(ret, b.data[m.roff+recMeta:endOff])
	}
	if mutate {
		m.roff = endOff
		m.size -= (readLen + recMeta)
		b.writemeta(m)
	}
	return ret, nil
}

// Peek reads the first record and returns it without removing it from the buffer.
func (b *Buffer) Peek() ([]byte, error) {
	return b.read(false)
}

// Pop removes and returns the first record in the buffer
func (b *Buffer) Pop() ([]byte, error) {
	return b.read(true)
}

func (b *Buffer) Size() int {
	b.Lock()
	m := b.readmeta()
	b.Unlock()
	return int(m.size)
}

// New creates a new Buffer backed by the file given by filename.
// it's max capacity is set to the capacity given
func New(filename string, capacity int) (*Buffer, error) {
	var (
		newFile bool
		f       *os.File
		err     error
	)

	// TODO: don't assume filename given is a good file.
	// TODO: get passed in open file?
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		f, err = os.Create(filename)
		if err != nil {
			return nil, err
		}
		newFile = true
	} else {
		f, err = os.OpenFile(filename, os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}
	}
	if err := syscall.Truncate(filename, int64(capacity)); err != nil {
		return nil, err
	}

	data, err := syscall.Mmap(
		int(f.Fd()), 0, capacity,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED,
	)
	if err != nil {
		return nil, err
	}
	// don't need this anymore
	f.Close()

	b := &Buffer{data: data}

	if newFile {
		m := meta{
			size: 0,
			woff: offData,
			roff: offData,
			cap:  uint64(capacity),
		}
		b.Lock()
		b.writemeta(m)
		b.Unlock()
	}

	return b, nil
}
