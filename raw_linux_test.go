// +build go1.12

package fifo

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestRawCopy(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "fifos")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpdir)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	f1, err := OpenFifo(ctx, filepath.Join(tmpdir, "f1"), syscall.O_RDWR|syscall.O_CREAT|syscall.O_NONBLOCK, 0600)
	assert.NoError(t, err)
	defer f1.Close()
	f1.(*fifo).noRawFallback = true

	f2, err := OpenFifo(ctx, filepath.Join(tmpdir, "f2"), syscall.O_RDWR|syscall.O_CREAT|syscall.O_NONBLOCK, 0600)
	assert.NoError(t, err)
	defer f2.Close()
	f2.(*fifo).noRawFallback = true

	chErr := make(chan error, 1)
	go func() {
		n, err := io.Copy(f2, f1)
		chErr <- errors.Wrapf(err, "error in copy, copied %d bytes", n)
	}()

	data := []byte("hello world!")
	_, err = f1.Write(data)
	assert.NoError(t, err)

	select {
	case err := <-chErr:
		t.Fatalf("copy returned early withe error: %v", err)
	default:
	}

	buf := make([]byte, len(data))
	_, err = io.ReadFull(f2, buf)
	assert.NoError(t, err)

	select {
	case err := <-chErr:
		t.Fatalf("copy returned early withe error: %v", err)
	default:
	}

	f1.Close()

	select {
	case err := <-chErr:
		assert.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for copy to finish")
	}
}

func BenchmarkRawCopy(b *testing.B) {
	benchmarkCopy(b, false)
}

func BenchmarkUserCopy(b *testing.B) {
	benchmarkCopy(b, true)
}

func benchmarkCopy(b *testing.B, forceUserCopy bool) {
	sizes := []int{8, 16, 32, 64, 1024, 1024 * 4, 1024 * 8, 1024 * 16, 1024 * 32, 1024 * 64, 1024 * 128, 1024 * 256}
	for _, size := range sizes {
		b.Run(strconv.Itoa(size)+"B", func(b *testing.B) {
			doBenchmarkCopy(b, size, forceUserCopy)
		})
	}
}

func doBenchmarkCopy(b *testing.B, size int, forceUserCopy bool) {
	b.StopTimer()

	tmpdir, err := ioutil.TempDir("", "fifos")
	assert.NoError(b, err)
	defer os.RemoveAll(tmpdir)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rw1, err := OpenFifo(ctx, filepath.Join(tmpdir, "f1"), syscall.O_RDWR|syscall.O_CREAT|syscall.O_NONBLOCK, 0600)
	assert.NoError(b, err)
	defer rw1.Close()
	f1 := rw1.(*fifo)
	f1.noRawFallback = !forceUserCopy
	f1.forceUserCopy = forceUserCopy

	setPipeSize(b, f1, size)

	rw2, err := OpenFifo(ctx, filepath.Join(tmpdir, "f2"), syscall.O_RDWR|syscall.O_CREAT|syscall.O_NONBLOCK, 0600)
	assert.NoError(b, err)
	defer rw2.Close()
	f2 := rw2.(*fifo)
	f2.noRawFallback = !forceUserCopy
	f2.forceUserCopy = forceUserCopy

	setPipeSize(b, f2, size)

	cancel()

	data := make([]byte, size-1)
	dataLen := int64(len(data))

	b.SetBytes(dataLen)
	b.ResetTimer()

	f1rc, err := f1.SyscallConn()
	assert.NoError(b, err)

	f2rc, err := f2.SyscallConn()
	assert.NoError(b, err)

	for i := 0; i < b.N; i++ {
		fillPipe(b, f1rc, data)

		// io.CopyN just calls io.Copy(w, io.LimitReader(r))
		// So we can just create the lmited reader here and avoid the allocation while
		// the timer is running.
		r := io.LimitReader(f1, dataLen)
		b.StartTimer()

		_, err = io.Copy(f2, r)
		if err != nil {
			b.Fatal(err)
		}

		b.StopTimer()
		drainPipe(b, f2rc)
	}

}

func drainPipe(tb testing.TB, rc syscall.RawConn) {
	buf := make([]byte, 32*1024)

	err := rc.Read(func(fd uintptr) bool {
		n, err := syscall.Read(int(fd), buf)
		if err != nil && err != syscall.EAGAIN {
			tb.Fatal(err)
		}
		return err != nil || n < len(buf)
	})

	assert.NoError(tb, err)
}

func fillPipe(tb testing.TB, w syscall.RawConn, data []byte) {
	tb.Helper()

	err := w.Write(func(fd uintptr) bool {
		var written int
		for {
			n, err := syscall.Write(int(fd), data[written:])
			written += n
			if err != nil {
				if err != syscall.EAGAIN {
					tb.Fatal(err)
				}
				return true
			}
			return written == len(data)
		}
	})

	if err != nil {
		tb.Fatal(err)
	}
}

func setPipeSize(tb testing.TB, f *fifo, size int) {
	tb.Helper()

	rc, err := f.SyscallConn()
	if err != nil {
		tb.Fatal(err)
	}

	err = rc.Control(func(fd uintptr) {
		_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_SETPIPE_SZ, uintptr(size))
		if errno > 0 {
			tb.Fatal(errno)
		}
	})
	if err != nil {
		tb.Fatal(err)
	}
}
