package main

import (
	"flag"
	"fmt"

	"github.com/cloudflare/buffer"
)

var (
	bufName = flag.String("filename", "", "location of the buffer")
)

func insert(b *buffer.Buffer, val string) {
	err := b.Insert([]byte(val))
	if err != nil {
		panic(err)
	}
}
func remove(b *buffer.Buffer) {
	w, err := b.Pop()
	if err != nil {
		panic(err)
	}
	fmt.Println("removed:", string(w))
}

func main() {
	flag.Parse()
	b, err := buffer.New(*bufName, 8*12)
	if err != nil {
		panic(err)
	}
	insert(b, "....................")
	insert(b, "......................")
	remove(b)
	remove(b)
	insert(b, "++++++++")
	// insert(b, "++++++++++++++++++++++++")
	// insert(b, "1234")
	// insert(b, "overflowthatbuffer")
	// remove()
}
