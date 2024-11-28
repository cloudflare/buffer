// +build windows,plan9,solaris

package buffer

import (
	"os"
)

func flock(f *os.File, exclusive bool) error {
	return nil //do nothing
}

func funlock(f *os.File) error {
	return nil
}
