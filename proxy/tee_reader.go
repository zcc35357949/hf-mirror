package proxy

import (
	"io"
)

var _ io.ReadCloser = (*teeReadCloser)(nil)

type teeReadCloser struct {
	r     io.ReadCloser
	w     io.WriteCloser
	teeRd io.Reader
}

func NewTeeReadCloser(r io.ReadCloser, w io.WriteCloser) io.ReadCloser {
	rd := io.TeeReader(r, w)
	return &teeReadCloser{
		r:     r,
		w:     w,
		teeRd: rd,
	}
}

func (t *teeReadCloser) Close() error {
	rerr := t.r.Close()
	werr := t.w.Close()
	if rerr != nil {
		return rerr
	}
	return werr
}

func (t *teeReadCloser) Read(p []byte) (n int, err error) {
	return t.teeRd.Read(p)
}
