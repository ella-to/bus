package bus

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestEncryption_EncodeDecodeBasic(t *testing.T) {
	tests := []struct {
		name      string
		secretKey string
		blockSize int
		input     string
	}{
		{
			name:      "simple string",
			secretKey: "my-secret-key",
			blockSize: 4096,
			input:     "hello world",
		},
		{
			name:      "empty string",
			secretKey: "my-secret-key",
			blockSize: 4096,
			input:     "",
		},
		{
			name:      "json data",
			secretKey: "another-secret",
			blockSize: 4096,
			input:     `{"id":"123","subject":"a.b.c","payload":{"key":"value"},"created_at":"2025-01-18T06:55:35-05:00"}`,
		},
		{
			name:      "large data",
			secretKey: "large-data-key",
			blockSize: 4096,
			input:     strings.Repeat("test data with some repetitive content ", 1000),
		},
		{
			name:      "special characters",
			secretKey: "special-chars-key",
			blockSize: 4096,
			input:     "test\n\r\t\\\"special chars!@#$%^&*()",
		},
		{
			name:      "unicode characters",
			secretKey: "unicode-key",
			blockSize: 4096,
			input:     "Hello 世界 🌍 مرحبا Привет",
		},
		{
			name:      "small block size",
			secretKey: "small-block-key",
			blockSize: 64,
			input:     "This tests encryption with a small block size to ensure multiple blocks are handled correctly.",
		},
		{
			name:      "large block size",
			secretKey: "large-block-key",
			blockSize: 65536,
			input:     strings.Repeat("Large block test data ", 100),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc := NewEncryption(tt.secretKey, tt.blockSize)

			// Encode
			reader := strings.NewReader(tt.input)
			encrypted, err := enc.Encode(reader)
			if err != nil {
				t.Fatalf("Encode() error = %v", err)
			}

			// Decode
			decrypted, err := enc.Decode(encrypted)
			if err != nil {
				t.Fatalf("Decode() error = %v", err)
			}

			// Read decrypted data
			var output bytes.Buffer
			_, err = io.Copy(&output, decrypted)
			if err != nil {
				t.Fatalf("Reading decrypted data error = %v", err)
			}

			// Compare
			if output.String() != tt.input {
				t.Errorf("Decrypted data doesn't match input.\nExpected length: %d\nGot length: %d", len(tt.input), output.Len())
			}
		})
	}
}

func TestEncryption_DifferentKeysProduceDifferentOutput(t *testing.T) {
	input := "test data to encrypt"

	enc1 := NewEncryption("secret-key-1", 4096)
	enc2 := NewEncryption("secret-key-2", 4096)

	// Encrypt with key 1
	encrypted1, err := enc1.Encode(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Encode with key1 error = %v", err)
	}

	var buf1 bytes.Buffer
	_, err = io.Copy(&buf1, encrypted1)
	if err != nil {
		t.Fatalf("Reading encrypted1 error = %v", err)
	}

	// Encrypt with key 2
	encrypted2, err := enc2.Encode(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Encode with key2 error = %v", err)
	}

	var buf2 bytes.Buffer
	_, err = io.Copy(&buf2, encrypted2)
	if err != nil {
		t.Fatalf("Reading encrypted2 error = %v", err)
	}

	// The encrypted outputs should be different (due to different keys and random nonces)
	if bytes.Equal(buf1.Bytes(), buf2.Bytes()) {
		t.Error("Different keys should produce different encrypted output")
	}
}

func TestEncryption_SameKeyCanDecrypt(t *testing.T) {
	input := "test data that will be encrypted and decrypted"
	secretKey := "shared-secret-key"

	enc1 := NewEncryption(secretKey, 4096)
	enc2 := NewEncryption(secretKey, 4096)

	// Encrypt with enc1
	encrypted, err := enc1.Encode(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Encode error = %v", err)
	}

	// Decrypt with enc2 (same key)
	decrypted, err := enc2.Decode(encrypted)
	if err != nil {
		t.Fatalf("Decode error = %v", err)
	}

	var output bytes.Buffer
	_, err = io.Copy(&output, decrypted)
	if err != nil {
		t.Fatalf("Reading decrypted data error = %v", err)
	}

	if output.String() != input {
		t.Errorf("Decryption with same key failed.\nExpected: %s\nGot: %s", input, output.String())
	}
}

func TestEncryption_WrongKeyCannotDecrypt(t *testing.T) {
	input := "sensitive data"

	enc1 := NewEncryption("correct-key", 4096)
	enc2 := NewEncryption("wrong-key", 4096)

	// Encrypt with correct key
	encrypted, err := enc1.Encode(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Encode error = %v", err)
	}

	// Try to decrypt with wrong key
	decrypted, err := enc2.Decode(encrypted)
	if err != nil {
		// Error during decode setup is acceptable
		return
	}

	// Read should fail with wrong key
	var output bytes.Buffer
	_, err = io.Copy(&output, decrypted)
	if err == nil && output.String() == input {
		t.Error("Decryption with wrong key should fail or produce different output")
	}
}

func TestEncryption_MultipleEncodeDecodeOperations(t *testing.T) {
	enc := NewEncryption("reusable-key", 4096)
	testData := []string{
		"first message",
		"second message with more content",
		"third message 你好世界",
	}

	for i, data := range testData {
		// Encrypt
		encrypted, err := enc.Encode(strings.NewReader(data))
		if err != nil {
			t.Fatalf("Encode %d error = %v", i, err)
		}

		// Decrypt
		decrypted, err := enc.Decode(encrypted)
		if err != nil {
			t.Fatalf("Decode %d error = %v", i, err)
		}

		var output bytes.Buffer
		_, err = io.Copy(&output, decrypted)
		if err != nil {
			t.Fatalf("Reading decrypted data %d error = %v", i, err)
		}

		if output.String() != data {
			t.Errorf("Round trip %d failed.\nExpected: %s\nGot: %s", i, data, output.String())
		}
	}
}

func TestEncryption_LargeData(t *testing.T) {
	enc := NewEncryption("large-data-encryption-key", 4096)

	// Test with 1MB of data
	largeData := strings.Repeat("The quick brown fox jumps over the lazy dog. ", 20000)

	encrypted, err := enc.Encode(strings.NewReader(largeData))
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	decrypted, err := enc.Decode(encrypted)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	var output bytes.Buffer
	_, err = io.Copy(&output, decrypted)
	if err != nil {
		t.Fatalf("Reading decrypted data error = %v", err)
	}

	if output.String() != largeData {
		t.Errorf("Large data encryption/decryption failed. Sizes: expected %d, got %d", len(largeData), output.Len())
	}
}

func TestEncryption_VariousBlockSizes(t *testing.T) {
	blockSizes := []int{64, 256, 1024, 4096, 8192, 16384, 65536}
	input := strings.Repeat("Block size test data. ", 500)

	for _, blockSize := range blockSizes {
		t.Run(string(rune(blockSize)), func(t *testing.T) {
			enc := NewEncryption("block-size-test-key", blockSize)

			encrypted, err := enc.Encode(strings.NewReader(input))
			if err != nil {
				t.Fatalf("Encode with blockSize %d error = %v", blockSize, err)
			}

			decrypted, err := enc.Decode(encrypted)
			if err != nil {
				t.Fatalf("Decode with blockSize %d error = %v", blockSize, err)
			}

			var output bytes.Buffer
			_, err = io.Copy(&output, decrypted)
			if err != nil {
				t.Fatalf("Reading decrypted data error = %v", err)
			}

			if output.String() != input {
				t.Errorf("BlockSize %d: decryption failed. Length expected %d, got %d", blockSize, len(input), output.Len())
			}
		})
	}
}

func TestNewEncryption(t *testing.T) {
	enc := NewEncryption("test-secret", 4096)
	if enc == nil {
		t.Fatal("NewEncryption() returned nil")
	}

	if enc.blockSize != 4096 {
		t.Errorf("Expected blockSize 4096, got %d", enc.blockSize)
	}

	// Verify the key is hashed (should be 32 bytes)
	if len(enc.secretKey) != 32 {
		t.Errorf("Expected secretKey length 32, got %d", len(enc.secretKey))
	}
}

func TestEncryption_HashProducesConsistentKey(t *testing.T) {
	// Same secret should produce same key
	enc1 := NewEncryption("consistent-secret", 4096)
	enc2 := NewEncryption("consistent-secret", 4096)

	if enc1.secretKey != enc2.secretKey {
		t.Error("Same secret should produce same key hash")
	}

	// Different secrets should produce different keys
	enc3 := NewEncryption("different-secret", 4096)

	if enc1.secretKey == enc3.secretKey {
		t.Error("Different secrets should produce different key hashes")
	}
}

// Benchmarks

func BenchmarkEncryption_Encode_Small(b *testing.B) {
	enc := NewEncryption("benchmark-key", 4096)
	data := "small test data for encryption benchmark"

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		reader := strings.NewReader(data)
		encrypted, err := enc.Encode(reader)
		if err != nil {
			b.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, encrypted)
	}
}

func BenchmarkEncryption_Encode_Medium(b *testing.B) {
	enc := NewEncryption("benchmark-key", 4096)
	data := strings.Repeat("medium test data for encryption benchmark ", 100)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		reader := strings.NewReader(data)
		encrypted, err := enc.Encode(reader)
		if err != nil {
			b.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, encrypted)
	}
}

func BenchmarkEncryption_Encode_Large(b *testing.B) {
	enc := NewEncryption("benchmark-key", 4096)
	data := strings.Repeat("large test data for encryption benchmark ", 10000)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		reader := strings.NewReader(data)
		encrypted, err := enc.Encode(reader)
		if err != nil {
			b.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, encrypted)
	}
}

func BenchmarkEncryption_Decode_Small(b *testing.B) {
	enc := NewEncryption("benchmark-key", 4096)
	data := "small test data for decryption benchmark"

	// Pre-encrypt the data
	encrypted, err := enc.Encode(strings.NewReader(data))
	if err != nil {
		b.Fatal(err)
	}

	var encryptedBuf bytes.Buffer
	_, err = io.Copy(&encryptedBuf, encrypted)
	if err != nil {
		b.Fatal(err)
	}
	encryptedBytes := encryptedBuf.Bytes()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		reader := bytes.NewReader(encryptedBytes)
		decrypted, err := enc.Decode(reader)
		if err != nil {
			b.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, decrypted)
	}
}

func BenchmarkEncryption_Decode_Medium(b *testing.B) {
	enc := NewEncryption("benchmark-key", 4096)
	data := strings.Repeat("medium test data for decryption benchmark ", 100)

	// Pre-encrypt the data
	encrypted, err := enc.Encode(strings.NewReader(data))
	if err != nil {
		b.Fatal(err)
	}

	var encryptedBuf bytes.Buffer
	_, err = io.Copy(&encryptedBuf, encrypted)
	if err != nil {
		b.Fatal(err)
	}
	encryptedBytes := encryptedBuf.Bytes()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		reader := bytes.NewReader(encryptedBytes)
		decrypted, err := enc.Decode(reader)
		if err != nil {
			b.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, decrypted)
	}
}

func BenchmarkEncryption_Decode_Large(b *testing.B) {
	enc := NewEncryption("benchmark-key", 4096)
	data := strings.Repeat("large test data for decryption benchmark ", 10000)

	// Pre-encrypt the data
	encrypted, err := enc.Encode(strings.NewReader(data))
	if err != nil {
		b.Fatal(err)
	}

	var encryptedBuf bytes.Buffer
	_, err = io.Copy(&encryptedBuf, encrypted)
	if err != nil {
		b.Fatal(err)
	}
	encryptedBytes := encryptedBuf.Bytes()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		reader := bytes.NewReader(encryptedBytes)
		decrypted, err := enc.Decode(reader)
		if err != nil {
			b.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, decrypted)
	}
}

func BenchmarkEncryption_RoundTrip_Small(b *testing.B) {
	enc := NewEncryption("benchmark-key", 4096)
	data := "small test data for round trip benchmark"

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		// Encrypt
		encrypted, err := enc.Encode(strings.NewReader(data))
		if err != nil {
			b.Fatal(err)
		}

		// Decrypt
		decrypted, err := enc.Decode(encrypted)
		if err != nil {
			b.Fatal(err)
		}

		_, _ = io.Copy(io.Discard, decrypted)
	}
}

func BenchmarkEncryption_RoundTrip_Medium(b *testing.B) {
	enc := NewEncryption("benchmark-key", 4096)
	data := strings.Repeat("medium test data for round trip benchmark ", 100)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		// Encrypt
		encrypted, err := enc.Encode(strings.NewReader(data))
		if err != nil {
			b.Fatal(err)
		}

		// Decrypt
		decrypted, err := enc.Decode(encrypted)
		if err != nil {
			b.Fatal(err)
		}

		_, _ = io.Copy(io.Discard, decrypted)
	}
}

func BenchmarkEncryption_RoundTrip_Large(b *testing.B) {
	enc := NewEncryption("benchmark-key", 4096)
	data := strings.Repeat("large test data for round trip benchmark ", 10000)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		// Encrypt
		encrypted, err := enc.Encode(strings.NewReader(data))
		if err != nil {
			b.Fatal(err)
		}

		// Decrypt
		decrypted, err := enc.Decode(encrypted)
		if err != nil {
			b.Fatal(err)
		}

		_, _ = io.Copy(io.Discard, decrypted)
	}
}

func BenchmarkEncryption_BlockSize_64(b *testing.B) {
	benchmarkWithBlockSize(b, 64)
}

func BenchmarkEncryption_BlockSize_256(b *testing.B) {
	benchmarkWithBlockSize(b, 256)
}

func BenchmarkEncryption_BlockSize_1024(b *testing.B) {
	benchmarkWithBlockSize(b, 1024)
}

func BenchmarkEncryption_BlockSize_4096(b *testing.B) {
	benchmarkWithBlockSize(b, 4096)
}

func BenchmarkEncryption_BlockSize_16384(b *testing.B) {
	benchmarkWithBlockSize(b, 16384)
}

func BenchmarkEncryption_BlockSize_65536(b *testing.B) {
	benchmarkWithBlockSize(b, 65536)
}

func benchmarkWithBlockSize(b *testing.B, blockSize int) {
	enc := NewEncryption("benchmark-key", blockSize)
	data := strings.Repeat("block size benchmark data ", 1000)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		encrypted, err := enc.Encode(strings.NewReader(data))
		if err != nil {
			b.Fatal(err)
		}

		decrypted, err := enc.Decode(encrypted)
		if err != nil {
			b.Fatal(err)
		}

		_, _ = io.Copy(io.Discard, decrypted)
	}
}

func BenchmarkEncryption_Overhead(b *testing.B) {
	enc := NewEncryption("benchmark-key", 4096)
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		data := strings.Repeat("x", size)
		b.Run(string(rune(size)), func(b *testing.B) {
			b.ReportAllocs()

			for b.Loop() {
				encrypted, err := enc.Encode(strings.NewReader(data))
				if err != nil {
					b.Fatal(err)
				}

				var buf bytes.Buffer
				_, _ = io.Copy(&buf, encrypted)

				b.ReportMetric(float64(buf.Len()-size), "overhead_bytes")
			}
		})
	}
}
