// package buffer provides an mmap'd circular buffer
package buffer

import (
	"bytes"
	"errors"
	"os"
	"sync"
	"syscall"
	"unsafe"

	"github.com/cloudflare/buffer/binary"
)

var (
	ErrNotOpen = errors.New("Not open")
	ErrEmpty   = errors.New("Empty file")
	ErrCorrupt = errors.New("Corrupt")
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
	f    *os.File
	data []byte
	m    *meta
}

type meta struct {
	magic [16]byte
	size  uint64
	woff  uint64
	roff  uint64
	cap   uint64
}

const (
	magicHeaderSize = 16
)

//CLOUDFLAREBUFFER
var magicHeader = [16]byte{0x43, 0x4c, 0x4f, 0x55, 0x44, 0x46, 0x4c, 0x41, 0x52, 0x45, 0x42, 0x55, 0x46, 0x45, 0x52}

const (
	offMagic       = 0
	offSize        = offMagic + magicHeaderSize
	offNextRead    = offSize + 8
	offNextWrite   = offNextRead + 8
	offMaxCapacity = offNextWrite + 8
	offData        = offMaxCapacity + 8
)

// recordLength contains metadata about what's stored.
// Currently only the recordLength
const recMeta = 8

func readmeta(f *os.File) (m meta, err error) {
	var fi os.FileInfo
	var buff []byte
	if f == nil {
		err = ErrNotOpen
	} else if fi, err = f.Stat(); err == nil {
		if sz := fi.Size(); sz == 0 {
			err = ErrEmpty
		} else if sz < offData {
			//not a new file but metdata is bad
			err = errors.New("corrupted file header")
		} else {
			var n int
			//check the magic bytes
			buff = make([]byte, offData)
			if n, err = f.ReadAt(buff, 0); err != nil {
				return
			} else if n != len(buff) {
				err = errors.New("Failed read")
			} else if !bytes.Equal(buff[0:magicHeaderSize], magicHeader[:]) {
				err = ErrCorrupt
			}
		}
	}
	if err != nil {
		return
	}
	//do the actual extration
	m = getMeta(buff)
	//check the capacity against the file size
	if uint64(fi.Size()) != m.cap {
		err = ErrCorrupt
	}

	return
}

func getMeta(b []byte) (m meta) {
	m = meta{
		magic: magicHeader,
		size:  binary.GetLittleEndianUint64(b, offSize),
		roff:  binary.GetLittleEndianUint64(b, offNextRead),
		woff:  binary.GetLittleEndianUint64(b, offNextWrite),
		cap:   binary.GetLittleEndianUint64(b, offMaxCapacity),
	}
	return
}

func putMeta(m meta, b []byte) {
	copy(b, magicHeader[:])
	binary.PutLittleEndianUint64(b, offSize, m.size)
	binary.PutLittleEndianUint64(b, offNextRead, m.roff)
	binary.PutLittleEndianUint64(b, offNextWrite, m.woff)
	binary.PutLittleEndianUint64(b, offMaxCapacity, m.cap)
}

func (b *Buffer) readmeta() (m meta) {
	b.Lock()
	m = *b.m
	b.Unlock()
	return
}

func (b *Buffer) writemeta(m meta) {
	b.Lock()
	*b.m = m
	b.Unlock()
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
func (b *Buffer) Insert(data []byte) (err error) {
	b.Lock()
	err = b.insert(data)
	b.Unlock()
	return
}

// InsertWithOverwrite inserts data into the buffer, potentially removing existing data
// Inserting using this function CAN overwrite data if there isn't enough free space
func (b *Buffer) InsertWithOverwrite(data []byte) (err error) {
	writeLen := uint64(len(data))
	// data is larger filesize - metadata
	if writeLen > (b.m.cap - offData - recMeta) {
		return errToBig
	}

	b.Lock()
	for writeLen > b.free() {
		if _, err = b.read(true); err != nil {
			break
		}
	}
	if err == nil {
		err = b.insert(data)
	}
	b.Unlock()
	return
}

func (b *Buffer) insert(data []byte) error {
	writeLen := uint64(len(data))

	// data is larger filesize - metadata
	if writeLen > (b.m.cap - offData - recMeta) {
		return errToBig
	}

	// data exceeds available space
	if b.m.size+writeLen+offData+recMeta > b.m.cap {
		return errOverflow
	}

	wrap := false
	copyTo := 0
	endOff := b.m.woff + writeLen + recMeta
	if endOff >= b.m.cap {
		// Get to the start of the file
		endOff = endOff % b.m.cap
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
	case b.m.roff < b.m.woff && b.m.roff < endOff && endOff < b.m.woff:
		return errOverwrite
	//         w       r e
	// [+++++++________++++]
	case b.m.woff < b.m.roff && endOff > b.m.roff:
		return errOverwrite
	//     e   w       r
	// [+++++++________++++]
	case b.m.woff < b.m.roff && endOff < b.m.woff:
		return errOverwrite
	}

	switch {
	case wrap && b.m.woff+recMeta > b.m.cap:
		tmp := serializeMeta(writeLen)
		left := b.m.cap - b.m.woff
		// startData := (b.m.woff+recMeta)%b.m.cap + offData
		copy(b.data[b.m.woff:b.m.cap], tmp[:left])
		copy(b.data[offData:offData+recMeta-left], tmp[left:])
		copy(b.data[offData+recMeta-left:endOff], data)
	case wrap:
		binary.PutLittleEndianUint64(b.data, int(b.m.woff), uint64(writeLen))
		copy(b.data[b.m.woff+recMeta:b.m.cap], data[:copyTo])
		copy(b.data[offData:endOff], data[copyTo:])
	default:
		// write doesn't wrap
		binary.PutLittleEndianUint64(b.data, int(b.m.woff), uint64(writeLen))
		copy(b.data[b.m.woff+recMeta:endOff], data)
	}
	b.m.woff = endOff
	b.m.size += (writeLen + recMeta)
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

	if b.m.size == 0 {
		return nil, nil
	}

	var (
		readLen uint64
		left    uint64
	)
	if b.m.roff+recMeta > b.m.cap {
		left = b.m.cap - b.m.roff
		tmp := make([]byte, recMeta)
		copy(tmp[:left], b.data[b.m.roff:b.m.cap])
		copy(tmp[left:], b.data[offData:offData+recMeta-left])
		readLen = deserializeMeta(tmp)
	} else {
		readLen = binary.GetLittleEndianUint64(b.data, int(b.m.roff))
	}

	var copyTo int
	endOff := b.m.roff + readLen + recMeta
	wrap := false
	if endOff > b.m.cap {
		// Get to the start of the file
		endOff = endOff % b.m.cap
		// save for later
		copyTo = int(readLen - endOff)
		// skip the metadata
		endOff = endOff + offData
		wrap = true
	}

	ret := make([]byte, readLen)

	switch {
	case wrap && b.m.roff+recMeta > b.m.cap:
		copy(ret, b.data[offData+recMeta-left:endOff])
	case wrap:
		copy(ret[:copyTo], b.data[b.m.roff+recMeta:b.m.cap])
		copy(ret[copyTo:], b.data[offData:endOff])
	default:
		copy(ret, b.data[b.m.roff+recMeta:endOff])
	}
	if mutate {
		b.m.roff = endOff
		b.m.size -= (readLen + recMeta)
	}
	return ret, nil
}

// Peek reads the first record and returns it without removing it from the buffer.
func (b *Buffer) Peek() (buff []byte, err error) {
	b.Lock()
	buff, err = b.read(false)
	b.Unlock()
	return
}

// Pop removes and returns the first record in the buffer
func (b *Buffer) Pop() (buff []byte, err error) {
	b.Lock()
	buff, err = b.read(true)
	b.Unlock()
	return
}

// Size returns the overall size of the held data
func (b *Buffer) Size() (v int) {
	b.Lock()
	v = int(b.m.size)
	b.Unlock()
	return
}

// Free returns how much free data there is in the buffer
// we account for the record metadata, so if the actual free is less
func (b *Buffer) Free() (v int) {
	b.Lock()
	v = int(b.free())
	b.Unlock()
	return
}

func (b *Buffer) free() (v uint64) {
	if v = b.m.cap - (b.m.size + offData); v < recMeta {
		v = 0
	} else {
		v -= recMeta
	}
	return
}

// New creates a new Buffer backed by the file given by filename.
// it's max capacity is set to the capacity given
func New(filename string, capacity int) (*Buffer, error) {
	var (
		newFile bool
		f       *os.File
		err     error
	)

	// TODO: get passed in open file?
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		f, err = os.Create(filename)
		if err != nil {
			return nil, err
		}
		newFile = true
	} else {
		if f, err = os.OpenFile(filename, os.O_RDWR, 0644); err != nil {
			return nil, err
		} else if _, err = readmeta(f); err != nil {
			f.Close()
			return nil, err
		}
	}
	if err := syscall.Truncate(filename, int64(capacity)); err != nil {
		f.Close()
		return nil, err
	}
	b, err := open(f, capacity)
	if err != nil {
		f.Close()
		return nil, err
	}
	if newFile {
		b.Lock()
		*b.m = meta{
			magic: magicHeader,
			size:  0,
			woff:  offData,
			roff:  offData,
			cap:   uint64(capacity),
		}
		b.Unlock()
	}

	return b, nil
}

// Open will open an existing Buffer backed by the given file
// If the file does not exist it will be created
// If the buffer is new it will be set to the given capacity
// if the file already exists the capacity is ignored and we use the existing capacity
func Open(filename string, capacity int) (*Buffer, error) {
	var (
		newFile bool
		f       *os.File
		err     error
		m       meta
	)

	if _, err := os.Stat(filename); os.IsNotExist(err) {
		if capacity <= 0 {
			return nil, errors.New("Bad capacity")
		}
		if f, err = os.Create(filename); err != nil {
			return nil, err
		}
		if err := syscall.Truncate(filename, int64(capacity)); err != nil {
			f.Close()
			return nil, err
		}
		newFile = true
		m = meta{
			size: 0,
			woff: offData,
			roff: offData,
			cap:  uint64(capacity),
		}
	} else {
		if f, err = os.OpenFile(filename, os.O_RDWR, 0644); err != nil {
			return nil, err
		} else if m, err = readmeta(f); err != nil {
			f.Close()
			return nil, err
		}
		capacity = int(m.cap)
	}
	b, err := open(f, capacity)
	if err != nil {
		f.Close()
		return nil, err
	}
	if newFile {
		b.Lock()
		*b.m = m
		b.Unlock()
	}

	return b, nil
}

func open(f *os.File, capacity int) (*Buffer, error) {
	data, err := syscall.Mmap(
		int(f.Fd()), 0, capacity,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED,
	)
	if err != nil {
		return nil, err
	}
	if err = flock(f, true); err != nil {
		return nil, err
	}
	b := &Buffer{
		f:    f,
		data: data,
		m:    (*meta)(unsafe.Pointer(&data[0])),
	}
	return b, nil
}

func (b *Buffer) Sync() (err error) {
	b.Lock()
	defer b.Unlock()
	//check that our data isn't nil
	if len(b.data) == 0 {
		return ErrNotOpen
	}
	//call a blocking msync
	base := uintptr(unsafe.Pointer(&b.data[0]))
	sz := uintptr(len(b.data))
	if _, _, errno := syscall.Syscall(syscall.SYS_MSYNC, base, sz, 0x4); errno != 0 {
		err = errno
	}
	return
}

func (b *Buffer) Close() (err error) {
	b.Lock()
	defer b.Unlock()
	//check that our data isn't nil
	if b.data == nil {
		return ErrNotOpen
	}
	funlock(b.f)
	//unmap and set the buffer to nil
	if err = syscall.Munmap(b.data); err == nil {
		err = b.f.Close()
	} else {
		b.f.Close()
	}
	b.data = nil
	return
}
