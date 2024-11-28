package buffer

import (
	"os"
	"syscall"
)

var pageSize int

func init() {
	pageSize = os.Getpagesize()
}

func (b *Buffer) Advise() error {
	b.Lock()
	defer b.Unlock()

	// Start of next page above write offset
	wo := int(b.m.woff) + (pageSize - int(b.m.woff)%pageSize)

	// Start of page below read offset
	ro := int(b.m.roff) - int(b.m.roff)%pageSize

	if ro > wo {
		return syscall.Madvise(b.data[wo:ro], syscall.MADV_DONTNEED)
	}

	if ro > pageSize {
		// don't clear the first page (containing metadata)
		err := syscall.Madvise(b.data[pageSize:ro], syscall.MADV_DONTNEED)
		if err != nil {
			return err
		}
	}
	if wo < int(b.m.cap) {
		return syscall.Madvise(b.data[wo:b.m.cap], syscall.MADV_DONTNEED)
	}
	return nil
}
