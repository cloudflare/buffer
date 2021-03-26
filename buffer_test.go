package buffer

import (
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

func TestAll(t *testing.T) {
	cases := []tc{
		tc{ // happy test
			ops: []operation{
				{"Wooorldd", write},
				{"Helloooo", write},
			},
			expect: meta{
				size: 32,
				woff: 32,
				roff: 32,
				cap:  64,
			},
		},
		tc{ // happy test
			ops: []operation{
				{"小洞不补, 大洞吃苦.", write},
				{"小洞不补, 大洞吃苦.", write},
				{"小洞不补, 大洞吃苦.", read},
			},
			expect: meta{
				size: 35,
				woff: 102,
				roff: 67,
				cap:  108,
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
				size: 0,
				woff: 64,
				roff: 64,
				cap:  70,
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
				size: 0,
				woff: 42,
				roff: 42,
				cap:  70,
			},
		},
	}

	for i, c := range cases {
		f, err := ioutil.TempFile("tests", "tmpBufTest")
		if err != nil {
			t.Fatalf("Unexpected failure creating temp file: %v", err)
		}
		// only want it for the name? TODO: figure a better way to do this
		filename := f.Name()
		err = os.Remove(filename)
		if err != nil {
			t.Fatalf("Unexpected failure removing temp file: %v", err)
		}
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
		os.Remove(filename)
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
				size: 21,
				woff: 53,
				roff: 32,
				cap:  64,
			},
		},
		tc{
			ops: []operation{
				{"小洞不补, 大洞吃苦. 小洞不补, 大洞吃苦.", write},
			},
			err: errToBig,
			expect: meta{
				size: 0,
				woff: 32,
				roff: 32,
				cap:  48,
			},
		},
		tc{
			ops: []operation{
				{"", read},
			},
			err: nil,
			expect: meta{
				size: 0,
				woff: 32,
				roff: 32,
				cap:  64,
			},
		},
	}

	for i, c := range cases {
		f, err := ioutil.TempFile("tests", "tmpBufTest")
		if err != nil {
			t.Fatalf("Unexpected failure creating temp file: %v", err)
		}

		// only want it for the name? TODO: figure a better way to do this
		filename := f.Name()
		err = os.Remove(filename)
		if err != nil {
			t.Fatalf("Unexpected failure removing temp file: %v", err)
		}
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
		os.Remove(filename)
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
			size: 21,
			woff: 53,
			roff: 32,
			cap:  64,
		},
	}

	f, err := ioutil.TempFile("tests", "tmpBufTest")
	if err != nil {
		t.Fatalf("Unexpected failure creating temp file: %v", err)
	}

	// only want it for the name? TODO: figure a better way to do this
	filename := f.Name()
	err = os.Remove(filename)
	if err != nil {
		t.Fatalf("Unexpected failure removing temp file: %v", err)
	}
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
			_, err := b.Pop()
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
	os.Remove(filename)
}

func TestAdvise(t *testing.T) {
	f, err := ioutil.TempFile("tests", "tmpBufTest")
	if err != nil {
		t.Fatalf("Unexpected failure creating temp file: %v", err)
	}

	// only want it for the name? TODO: figure a better way to do this
	filename := f.Name()
	err = os.Remove(filename)
	if err != nil {
		t.Fatalf("Unexpected failure removing temp file: %v", err)
	}

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

	os.Remove(filename)
}

// benchBuffer tests reading/writing count batches of size batch in sequential chunks
// with file capacity cap
func benchBuffer(cap, recordSize int, b *testing.B) {

	f, err := ioutil.TempFile("tests", "tmpBufTest")
	if err != nil {
		b.Fatalf("Unexpected failure creating temp file: %v", err)
	}

	// only want it for the name? TODO: figure a better way to do this
	filename := f.Name()
	err = os.Remove(filename)
	if err != nil {
		b.Fatalf("Unexpected failure removing temp file: %v", err)
	}
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

	os.Remove(filename)
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
