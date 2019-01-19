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

	return rawCopy(wrc, rrc, copyN)
}

var bufPool = sync.Pool{New: func() interface{} { return make([]byte, 32*1024) }}

func getBuffer() []byte {
	return bufPool.Get().([]byte)
}

func putBuffer(buf []byte) {
	bufPool.Put(buf)
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

func rawCopy(w syscall.RawConn, r syscall.RawConn, remain int64) (copied int64, retErr error) {
	var (
		// send fd's from read/write methods
		chRfd = make(chan uintptr)
		chWfd = make(chan uintptr)

		// syncronizes both the read/write gorotines with the actual copy loop
		chErr = make(chan error, 2)

		// store errors from splice call
		copyErr error
	)

	// These goroutines call the pipes' `Read` and `Write` methods which run
	// the passed in function (handler) when the file descriptor is ready for read or write,
	// respectively..
	//
	// The handlers send the fd (which is stable for the entire handler call) to
	// the loop which runs splice(2).
	// From there the handlers wait for the call to splice(2) to finish and determine
	// how to proceed based on the error sent back.
	//
	// The handlers should return true only when we are done with reads or writes.
	//
	// The error passed back to the goroutines from splice should never be nil.
	// (otherwise we'd have just looped again to do another splice(2))
	//
	// The error will be `EAGAIN` whhen a pipe is no longer ready (e.g. pipe
	// full), in which case the handler will return false, from there the raw
	// conn will sleep until the pipe is ready again, and then the function
	// will be run again.
	// There is no way for us to know which fd gave us the EAGIN, so we assume
	// that we need to wait for both fd's.
	//
	// Any other error is a fatal error and we should stop everything.
	//
	// *Note*: when either pipe is closed, we will get an error from Go's internal/poll package.
	// This error is not something we can handle (because it is internal to Go).
	// This is a known issue and will not be fixed upstream.
	go func() {
		err := w.Write(func(fd uintptr) bool {
			select {
			case chWfd <- fd:
			case err := <-chErr:
				return remain == 0 || (err != nil && err != syscall.EAGAIN)
			}

			err := <-chErr // wait for splice loop to finish
			return remain == 0 || (err != nil && err != syscall.EAGAIN)
		})
		if err != nil {
			err = errors.Wrap(err, "write error")
		}
		chErr <- err
	}()

	go func() {
		err := r.Read(func(fd uintptr) bool {
			select {
			case chRfd <- fd:
			case err := <-chErr:
				return remain == 0 || (err != nil && err != syscall.EAGAIN)
			}

			err := <-chErr // wait for splice loop to finish
			return remain == 0 || (err != nil && err != syscall.EAGAIN)
		})
		if err != nil {
			err = errors.Wrap(err, "read error")
		}
		chErr <- err
	}()

	var wfd, rfd uintptr
	defer func() {
		chErr <- retErr
		chErr <- retErr
	}()

	for {
		if remain == 0 {
			break
		}
		// wait for read/write fd's.
		select {
		case wfd = <-chWfd:
		case rfd = <-chRfd:
		case err := <-chErr:
			return copied, err
		}
		select {
		case wfd = <-chWfd:
		case rfd = <-chRfd:
		case err := <-chErr:
			return copied, err
		}

		var (
			err error
			n   int64
		)
		// inner loop lets us call splice over and over on the same fd's until we
		// get an error.
		for {
			select {
			case err := <-chErr:
				chErr <- err
				chErr <- err
				return copied, err
			default:
			}

			n, err = syscall.Splice(int(rfd), nil, int(wfd), nil, int(remain), spliceMove|spliceNonblock)
			copied += n
			remain -= n
			if err != nil || remain == 0 {
				break
			}
		}

		// signal goroutines that splice is finished
		// This error should never be nil
		chErr <- err
		chErr <- err

		// EAGAIN just means one of our fd's is not ready anymore and we need to
		// let the poller wait until they are ready again.
		// Any other error is fatal.
		//
		// We'll need to wait for new fd's after EAGAIN due to the interface guarentees
		// from syscall.RawConn
		if err != syscall.EAGAIN {
			copyErr = err
			break
		}
	}

	if copyErr != nil {
		return copied, copyErr
	}
	if err := <-chErr; err != nil {
		return copied, err
	}
	if err := <-chErr; err != nil {
		return copied, err
	}
	return copied, nil
}
