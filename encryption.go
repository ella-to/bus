package bus

import (
	"crypto/sha256"
	"io"

	"ella.to/crypto"
)

type Encryption struct {
	secretKey [32]byte
	blockSize int
}

func (e *Encryption) Encode(r io.Reader) (io.Reader, error) {
	pr, pw := io.Pipe()

	go func() {
		_, err := crypto.EncryptStream(e.secretKey, e.blockSize, pw, r)
		if err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		_ = pw.Close()
	}()

	return pr, nil
}

func (e *Encryption) Decode(r io.Reader) (io.Reader, error) {
	pr, pw := io.Pipe()

	go func() {
		_, err := crypto.DecryptStream(e.secretKey, e.blockSize, pw, r)
		if err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		_ = pw.Close()
	}()

	return pr, nil
}

func NewEncryption(secretKey string, blockSize int) *Encryption {
	return &Encryption{
		secretKey: sha256.Sum256([]byte(secretKey)),
		blockSize: blockSize,
	}
}
