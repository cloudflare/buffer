// +build !windows,!plan9,!solaris

//this package is based on the flock implementation used in boltdb
//which is MIT licensed and available at:
//	https://github.com/boltdb/bolt/blob/master/bolt_unix.go

package buffer

import (
	"errors"
	"os"
	"syscall"
)

var (
	ErrTimeout = errors.New("Timeout")
	ErrLocked  = errors.New("File is already locked")
)

//flock locks a file for this process, this DOES NOT prevent the same process
//from opening the
func flock(f *os.File, exclusive bool) error {
	var lock syscall.Flock_t
	lock.Start = 0
	lock.Len = 0
	lock.Pid = 0
	lock.Whence = 0
	lock.Pid = 0
	if exclusive {
		lock.Type = syscall.F_WRLCK
	} else {
		lock.Type = syscall.F_RDLCK
	}
	err := rawFdCall(f, func(fd uintptr) error {
		return syscall.FcntlFlock(fd, syscall.F_SETLK, &lock)
	})
	if err == nil {
		return nil
	} else if err == syscall.EAGAIN {
		return ErrLocked
	}
	return err
}

//funlock releases a lock held on a file descriptor
func funlock(f *os.File) error {
	var lock syscall.Flock_t
	lock.Start = 0
	lock.Len = 0
	lock.Type = syscall.F_UNLCK
	lock.Whence = 0
	return rawFdCall(f, func(fd uintptr) error {
		return syscall.FcntlFlock(fd, syscall.F_SETLK, &lock)
	})
}

type controlFunc func(fd uintptr) error

func rawFdCall(fio *os.File, cf controlFunc) error {
	if fio == nil || cf == nil {
		return errors.New("invalid parameters")
	}
	rc, err := fio.SyscallConn()
	if err != nil {
		return err
	}
	rc.Control(func(fd uintptr) {
		err = cf(fd)
	})
	return err
}
