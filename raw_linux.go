// +build go1.12

package fifo

import (
	"io"
	"strings"
	"sync"
	"syscall"

	"github.com/pkg/errors"
)

const (
	spliceMove     = 0x1
	spliceNonblock = 0x2
	maxCopy        = int64(1<<63 - 1)
)

// ReadFrom implements io.ReaderFrom
// This allows io.Copy to take advantage of in-kernel copying
func (f *fifo) ReadFrom(r io.Reader) (int64, error) {
	if !f.forceUserCopy {
		return f.rawReadFrom(r)
	}
	return userCopy(f, r)
}

// WriteTo implements io.WriterTo
// This allows io.Copy to take advantage of in-kernel copying
//func (f *fifo) WriteTo(w io.Writer) (int64, error) {
//	if !f.forceUserCopy {
//		return f.rawWriteTo(w)
//	}
//	return io.Copy(w, &wrapUserCopy{f})
//}

func (f *fifo) rawReadFrom(r io.Reader) (int64, error) {
	copyN := maxCopy
	if lr, ok := r.(*io.LimitedReader); ok {
		r = lr.R
		copyN = lr.N
	}
	rsc, ok := r.(syscall.Conn)
	if !ok {
		return userCopy(f, r)
	}

	rrc, err := rsc.SyscallConn()
	if err != nil {
		if f.noRawFallback {
			return 0, err
		}
		return userCopy(f, r)
	}

	wrc, err := f.SyscallConn()
	if err != nil {
		if f.noRawFallback {
			return 0, err
		}
		return userCopy(f, r)
	}

	n, err := rawCopy(wrc, rrc, copyN)
	if err != nil {
		if strings.Contains(err.Error(), "use of closed file") {
			return n, nil
		}
	}
	return n, err
}

var bufPool = sync.Pool{New: func() interface{} { return make([]byte, 32*1024) }}

func getBuffer() []byte {
	return bufPool.Get().([]byte)
}

func putBuffer(buf []byte) {
	bufPool.Put(buf)
}

func (f *fifo) rawWriteTo(w io.Writer) (int64, error) {
	wsc, ok := w.(syscall.Conn)
	if !ok {
		return userCopy(w, f)
	}

	wrc, err := wsc.SyscallConn()
	if err != nil {
		if f.noRawFallback {
			return 0, errors.Wrap(err, "error making syscall.Conn")
		}
		return userCopy(w, f)
	}

	rrc, err := f.SyscallConn()
	if err != nil {
		if f.noRawFallback {
			return 0, errors.Wrap(err, "error making syscall.Conn")
		}
		return userCopy(w, f)
	}

	n, err := rawCopy(wrc, rrc, maxCopy)
	if err != nil {
		select {
		case <-f.closed:
			err = nil
		default:
		}
	}

	if err != nil {
		if strings.Contains(err.Error(), "use of closed file") {
			return n, nil
		}
	}
	return n, err
}

func rawCopy(w syscall.RawConn, r syscall.RawConn, remain int64) (int64, error) {
	var (
		wErr, rErr, copyErr error
		copied              int64
	)

	// When the passed in handler functions are called this signals that the
	// file descriptor is ready.
	//
	// `syscall.Splice` will return `EAGAIN` when either side is no longer ready.
	// e.g. if the write pipe is full or the read pipe is empty.
	// When this happens we'll need to wait for them to be ready again.
	// Normally we would just return `false` in the read/write, but we don't know
	// which fd is not ready and since these are nested we can't trigger the
	// read side to wait to be ready without breaking out of the write.
	//
	// Remember, when the Read/Write handlers return `false`, this does *not*
	// cause the calls to Read/Write to return, but rather wait for the file to
	// be ready, at which time the handler will be called again.
	// Only when the handlers return `true` does this trigger Read/Write to return.
	//
	// Any other error than `EAGAIN` is a fatal error. `EAGAIN` means we need to
	// wait for the files to be ready, this is the only case where we will return
	// false, and only in the `Read` handler since these calls are nested.
	rErr = r.Read(func(rfd uintptr) bool {
		wErr = w.Write(func(wfd uintptr) bool {
			var n int64
			for remain > 0 && copyErr == nil {
				n, copyErr = syscall.Splice(int(rfd), nil, int(wfd), nil, int(remain), spliceMove|spliceNonblock)
				if n > 0 {
					copied += n
					remain -= n
				}
			}
			return copyErr != nil || remain == 0
		})
		return remain == 0 || (copyErr != nil && copyErr != syscall.EAGAIN)
	})

	if wErr != nil {
		return copied, errors.Wrap(wErr, "write error")
	}
	if rErr != nil {
		return copied, errors.Wrap(rErr, "read error")
	}
	if copyErr != nil && copyErr != syscall.EAGAIN {
		return copied, errors.Wrap(copyErr, "splice error")
	}

	return copied, nil
}

func userCopy(dst io.Writer, src io.Reader) (written int64, err error) {
	buf := getBuffer()

	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}

	putBuffer(buf)
	return written, err
}
