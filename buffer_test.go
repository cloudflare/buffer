package buffer

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

const (
	write = true
	read  = false
)

type operation struct {
	value string
	write bool
}

type tc struct {
	ops    []operation
	expect meta
	err    error
}

var (
	td string
)

func TestMain(m *testing.M) {
	var err error
	if td, err = ioutil.TempDir(os.TempDir(), "buffer"); err != nil {
		fmt.Println("Failed to make temporary directory", err)
		os.Exit(-1)
	}
	r := m.Run()
	os.RemoveAll(td)
	os.Exit(r)
}

func TestAll(t *testing.T) {
	cases := []tc{
		tc{ // happy test
			ops: []operation{
				{"Wooorldd", write},
				{"Helloooo", write},
			},
			expect: meta{
				magic: magicHeader,
				size:  32,
				woff:  48,
				roff:  48,
				cap:   80,
			},
		},
		tc{ // happy test
			ops: []operation{
				{"小洞不补, 大洞吃苦.", write},
				{"小洞不补, 大洞吃苦.", write},
				{"小洞不补, 大洞吃苦.", read},
			},
			expect: meta{
				magic: magicHeader,
				size:  35,
				woff:  118,
				roff:  83,
				cap:   124,
			},
		},
		tc{ // Tests read/write for recMeta wrap
			ops: []operation{
				{"小洞不补, 大洞吃苦.", write},
				{"小洞不补, 大洞吃苦.", read},
				{"小洞不补, 大洞吃苦.", write},
				{"小洞不补, 大洞吃苦.", read},
			},
			expect: meta{
				magic: magicHeader,
				size:  0,
				woff:  80,
				roff:  80,
				cap:   86,
			},
		},
		tc{ // Tests read/write for data wrapping
			ops: []operation{
				{"Hello", write},
				{"Hello", read},
				{"小洞不补, 大洞吃苦.", write},
				{"小洞不补, 大洞吃苦.", read},
			},
			expect: meta{
				magic: magicHeader,
				size:  0,
				woff:  58,
				roff:  58,
				cap:   86,
			},
		},
	}
	for i, c := range cases {
		filename := tempFileName(t) //located in testing.T tempdir, no need to remove
		b, err := New(filename, int(c.expect.cap))
		if err != nil {
			t.Errorf("Unexpected failure creating buffer for case: %d", i)
		}
		for _, op := range c.ops {
			if op.write {
				err := b.Insert([]byte(op.value))
				if err != nil {
					t.Errorf("Unexpected Insert failure for case %d. err: %v", i, err)
				}
			} else {
				val, err := b.Pop()
				if err != nil {
					t.Errorf("Unexpected Pop failure for case %d. err: %v", i, err)
				}
				if string(val) != op.value {
					t.Errorf("Values do not match for case %d. expected: %v actual: %v", i, op.value, val)
				}
				t.Logf("%v", string(val))
			}
		}
		if err = b.Sync(); err != nil {
			t.Errorf("Failed to sync: %v", err)
		}
		m := b.readmeta()
		if c.expect != m {
			t.Errorf("metadata structs do not match for %d. expect: %v, actual: %v", i, c.expect, m)
		}
		if err = b.Close(); err != nil {
			t.Errorf("Failed to close: %v", err)
		}

		//re-open the file with Open
		if b, err = Open(filename, int(c.expect.cap)); err != nil {
			t.Errorf("Failed to re-open: %v", err)
		}
		if m = b.readmeta(); c.expect != m {
			t.Errorf("metadata structs do not match for %d. expect: %v, actual: %v", i, c.expect, m)
		}
		if err = b.Close(); err != nil {
			t.Errorf("Failed to close: %v", err)
		}

		//re-open the file with Open and an expansion
		if b, err = Open(filename, int(c.expect.cap+1024)); err != nil {
			t.Errorf("Failed to re-open: %v", err)
		}
		m.cap += 1024
		if m = b.readmeta(); c.expect != m {
			t.Errorf("metadata structs do not match for %d. expect: %v, actual: %v", i, c.expect, m)
		}
		if err = b.Close(); err != nil {
			t.Errorf("Failed to close: %v", err)
		}

		//test open close with no capacity specified
		if b, err = Open(filename, 0); err != nil {
			t.Errorf("Failed to re-open: %v", err)
		} else if err = b.Close(); err != nil {
			t.Errorf("Failed to close: %v", err)
		}

	}
}

func TestErrors(t *testing.T) {
	cases := []tc{
		tc{
			ops: []operation{
				{"小洞不补,", write},
				{"大洞吃苦.", write},
			},
			err: errOverflow,
			expect: meta{
				magic: magicHeader,
				size:  21,
				woff:  69,
				roff:  48,
				cap:   80,
			},
		},
		tc{
			ops: []operation{
				{"小洞不补, 大洞吃苦. 小洞不补, 大洞吃苦.", write},
			},
			err: errToBig,
			expect: meta{
				magic: magicHeader,
				size:  0,
				woff:  48,
				roff:  48,
				cap:   64,
			},
		},
		tc{
			ops: []operation{
				{"", read},
			},
			err: nil,
			expect: meta{
				magic: magicHeader,
				size:  0,
				woff:  48,
				roff:  48,
				cap:   80,
			},
		},
	}
	for i, c := range cases {
		filename := tempFileName(t) //located in testing.T tempdir, no need to remove
		b, err := New(filename, int(c.expect.cap))
		if err != nil {
			t.Errorf("Unexpected failure creating buffer for case: %d", i)
		}

		for _, op := range c.ops {
			if op.write {
				err := b.Insert([]byte(op.value))
				switch err {
				case c.err:
					break
				case nil:
				default:
					t.Errorf("Unexpected Insert failure for case %d. err: %v", i, err)
				}
			} else {
				_, err := b.Pop()
				switch err {
				case c.err:
					break
				case nil:
				default:
					t.Errorf("Unexpected Pop failure for case %d. err: %v", i, err)
				}
			}
		}
		m := b.readmeta()
		if c.expect != m {
			t.Errorf("metadata structs do not match for %d. expect: %v, actual: %v", i, c.expect, m)
		}
	}
}

func TestHelpers(t *testing.T) {

	c := tc{
		ops: []operation{
			{"小洞不补,", write},
			{"大洞吃苦.", write},
		},
		err: errOverflow,
		expect: meta{
			magic: magicHeader,
			size:  21,
			woff:  69,
			roff:  48,
			cap:   80,
		},
	}
	filename := tempFileName(t) //located in testing.T tempdir, no need to remove
	b, err := New(filename, int(c.expect.cap))
	if err != nil {
		t.Errorf("Unexpected failure creating buffer: %v", err)
	}

	for _, op := range c.ops {
		if op.write {
			err := b.Insert([]byte(op.value))
			switch err {
			case c.err:
				break
			case nil:
			default:
				t.Errorf("Unexpected Insert failure. err: %v", err)
			}
		} else {
			_, err = b.Pop()
			switch err {
			case c.err:
				break
			case nil:
			default:
				t.Errorf("Unexpected Pop failure. err: %v", err)
			}
		}
	}
	m := b.readmeta()
	if c.expect != m {
		t.Errorf("metadata structs do not match. expect: %v, actual: %v", c.expect, m)
	}
	if b.Size() != int(m.size) {
		t.Errorf("Reported size does not match metadata")
	}
}

func TestAdvise(t *testing.T) {
	filename := tempFileName(t) //located in testing.T tempdir, no need to remove
	recordSize := 1000
	// 20 records precisely
	cap := (20*(recordSize+recMeta) + offData)

	buf, err := New(filename, cap)
	if err != nil {
		t.Errorf("Unexpected failure creating buffer.")
	}
	record := strings.Repeat("A", recordSize)

	// Test split advise case ro < wo
	for i := 0; i < 14; i++ {
		buf.Insert([]byte(record))
	}
	for i := 0; i < 10; i++ {
		buf.Pop()
	}
	err = buf.Advise()
	if err != nil {
		t.Errorf("unexpected error in advise: %v", err)
	}

	// Test straight advise case ro < wo
	for i := 0; i < 8; i++ {
		buf.Insert([]byte(record))
	}
	for i := 0; i < 8; i++ {
		buf.Pop()
	}
	err = buf.Advise()
	if err != nil {
		t.Errorf("unexpected error in advise: %v", err)
	}
}

func TestOverlap(t *testing.T) {
	c := tc{ // happy test
		ops: []operation{
			{"Wooorldd", write},
			{"Helloooo", write},
			{"Helloooo", write},
		},
		expect: meta{
			magic: magicHeader,
			size:  32,
			woff:  64, //do to the loop
			roff:  64,
			cap:   80,
		},
	}
	filename := tempFileName(t) //located in testing.T tempdir, no need to remove
	b, err := New(filename, int(c.expect.cap))
	if err != nil {
		t.Errorf("Unexpected failure creating buffer")
	}
	for _, op := range c.ops {
		if op.write {
			err := b.InsertWithOverwrite([]byte(op.value))
			if err != nil {
				t.Errorf("Unexpected Insert failure: %v", err)
			}
		} else {
			val, err := b.Pop()
			if err != nil {
				t.Errorf("Unexpected Pop failure: %v", err)
			}
			if string(val) != op.value {
				t.Errorf("Values do not match expected: %v actual: %v", op.value, val)
			}
			t.Logf("%v", string(val))
		}
	}
	if err = b.Sync(); err != nil {
		t.Errorf("Failed to sync: %v", err)
	}
	m := b.readmeta()
	if c.expect != m {
		t.Errorf("metadata structs do not match expect: %v, actual: %v", c.expect, m)
	}

	//make sure we can only pop 2
	var buffs []string
	for {
		if buff, err := b.Pop(); err != nil {
			t.Errorf("Failed to pop: %v", err)
		} else if buff == nil {
			break
		} else {
			buffs = append(buffs, string(buff))
		}
	}
	if err = b.Close(); err != nil {
		t.Errorf("Failed to close: %v", err)
	}
	if len(buffs) != 2 {
		t.Errorf("Too many entries came back out: %d != 2", len(buffs))
	}
	if buffs[0] != string(c.ops[1].value) || buffs[1] != string(c.ops[2].value) {
		t.Errorf("invalid output: %v", buffs)
	}
}

// benchBuffer tests reading/writing count batches of size batch in sequential chunks
// with file capacity cap
func benchBuffer(cap, recordSize int, b *testing.B) {
	filename := tempFileName(b) //located in testing.T tempdir, no need to remove

	buf, err := New(filename, int(cap))
	if err != nil {
		b.Errorf("Unexpected failure creating buffer.")
	}

	record := strings.Repeat("A", recordSize)

	for z := 0; z < b.N; z++ {
		err := buf.Insert([]byte(record))
		if err != nil {
			b.Fatalf("Unexpected error while inserting: %v", err)
		}
		_, err = buf.Pop()
		if err != nil {
			b.Fatalf("Unexpected error while popping: %v", err)
		}
	}
}

func BenchmarkBuffer50KBCap1KBRec(b *testing.B) {
	// 50k file, 1KB record
	benchBuffer(50*1000, 1000, b)
}

func BenchmarkBuffer50KBCap100BRec(b *testing.B) {
	// 50k file, 100B record
	benchBuffer(50*1000, 100, b)
}

func BenchmarkBuffer500MBCap1KBRec(b *testing.B) {
	// 500MB file, 1KB record, 1/10th write/read, 5x
	benchBuffer(500*1000*1000, 1000, b)
}

func BenchmarkBuffer500MBCap1MBRec(b *testing.B) {
	// 500MB file, 1MB record
	benchBuffer(500*1000*1000, 1000*1000, b)
}

func tempFileName(t ft) (s string) {
	f, err := ioutil.TempFile(td, "tmpBufTest")
	if err != nil {
		t.Fatalf("Unexpected failure creating temp file: %v", err)
	}
	s = f.Name()
	if err = f.Close(); err != nil {
		t.Fatalf("Failed to close file: %v", err)
	} else if err = os.Remove(s); err != nil {
		t.Fatalf("Unexpected failure removing temp file: %v", err)
	}
	return
}

type ft interface {
	Fatalf(f string, args ...interface{})
}
