package compress

import (
	"bytes"
	"io"

	"ella.to/immuta"
	"github.com/klauspost/compress/s2"
)

type S2Compressor struct{}

var _ immuta.Compressor = (*S2Compressor)(nil)

func (c *S2Compressor) Compress(r io.Reader) (io.Reader, error) {
	var buf bytes.Buffer
	w := s2.NewWriter(&buf)

	_, err := io.Copy(w, r)
	if err != nil {
		w.Close()
		return nil, err
	}

	err = w.Close()
	if err != nil {
		return nil, err
	}

	return &buf, nil
}

func (c *S2Compressor) Decompress(r io.Reader) (io.Reader, error) {
	return s2.NewReader(r), nil
}

func NewS2Compressor() *S2Compressor {
	return &S2Compressor{}
}
