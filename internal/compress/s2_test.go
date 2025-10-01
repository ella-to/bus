package compress

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestS2Compressor_CompressDecompress(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:    "simple string",
			input:   "hello world",
			wantErr: false,
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: false,
		},
		{
			name:    "json data",
			input:   `{"id":"123","subject":"a.b.c","payload":{"key":"value"},"created_at":"2025-01-18T06:55:35-05:00"}`,
			wantErr: false,
		},
		{
			name:    "large data",
			input:   strings.Repeat("test data with some repetitive content ", 1000),
			wantErr: false,
		},
		{
			name:    "special characters",
			input:   "test\n\r\t\\\"special chars!@#$%^&*()",
			wantErr: false,
		},
		{
			name:    "unicode characters",
			input:   "Hello 世界 🌍 مرحبا Привет",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compressor := NewS2Compressor()

			// Compress
			reader := strings.NewReader(tt.input)
			compressed, err := compressor.Compress(reader)
			if (err != nil) != tt.wantErr {
				t.Errorf("Compress() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr {
				return
			}

			// Decompress
			decompressed, err := compressor.Decompress(compressed)
			if err != nil {
				t.Errorf("Decompress() error = %v", err)
				return
			}

			// Read decompressed data
			var output bytes.Buffer
			_, err = io.Copy(&output, decompressed)
			if err != nil {
				t.Errorf("Reading decompressed data error = %v", err)
				return
			}

			// Compare
			if output.String() != tt.input {
				t.Errorf("Decompressed data doesn't match input.\nExpected: %q\nGot: %q", tt.input, output.String())
			}
		})
	}
}

func TestS2Compressor_CompressEmptyReader(t *testing.T) {
	compressor := NewS2Compressor()
	reader := strings.NewReader("")
	
	compressed, err := compressor.Compress(reader)
	if err != nil {
		t.Fatalf("Compress() error = %v", err)
	}

	// Decompress empty data
	decompressed, err := compressor.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress() error = %v", err)
	}

	var output bytes.Buffer
	_, err = io.Copy(&output, decompressed)
	if err != nil {
		t.Fatalf("Reading decompressed data error = %v", err)
	}

	if output.String() != "" {
		t.Errorf("Expected empty string, got %q", output.String())
	}
}

func TestS2Compressor_MultipleCompress(t *testing.T) {
	compressor := NewS2Compressor()
	testData := "test data for multiple compressions"

	// First compression
	compressed1, err := compressor.Compress(strings.NewReader(testData))
	if err != nil {
		t.Fatalf("First Compress() error = %v", err)
	}

	// Second compression on same compressor instance
	compressed2, err := compressor.Compress(strings.NewReader(testData))
	if err != nil {
		t.Fatalf("Second Compress() error = %v", err)
	}

	// Both should decompress correctly
	for i, compressed := range []io.Reader{compressed1, compressed2} {
		decompressed, err := compressor.Decompress(compressed)
		if err != nil {
			t.Fatalf("Decompress %d error = %v", i+1, err)
		}

		var output bytes.Buffer
		_, err = io.Copy(&output, decompressed)
		if err != nil {
			t.Fatalf("Reading decompressed data %d error = %v", i+1, err)
		}

		if output.String() != testData {
			t.Errorf("Decompression %d failed. Expected: %q, Got: %q", i+1, testData, output.String())
		}
	}
}

func TestS2Compressor_CompressionRatio(t *testing.T) {
	compressor := NewS2Compressor()
	
	// Highly repetitive data should compress well
	input := strings.Repeat("test ", 1000)
	reader := strings.NewReader(input)
	
	compressed, err := compressor.Compress(reader)
	if err != nil {
		t.Fatalf("Compress() error = %v", err)
	}

	// Read compressed data to check size
	var compressedBuf bytes.Buffer
	_, err = io.Copy(&compressedBuf, compressed)
	if err != nil {
		t.Fatalf("Reading compressed data error = %v", err)
	}

	originalSize := len(input)
	compressedSize := compressedBuf.Len()

	// Compressed size should be significantly smaller for repetitive data
	if compressedSize >= originalSize {
		t.Logf("Warning: Compressed size (%d) is not smaller than original (%d)", compressedSize, originalSize)
	} else {
		ratio := float64(originalSize) / float64(compressedSize)
		t.Logf("Compression ratio: %.2fx (original: %d bytes, compressed: %d bytes)", ratio, originalSize, compressedSize)
	}
}

func TestS2Compressor_LargeData(t *testing.T) {
	compressor := NewS2Compressor()
	
	// Test with 1MB of data
	largeData := strings.Repeat("The quick brown fox jumps over the lazy dog. ", 20000)
	
	compressed, err := compressor.Compress(strings.NewReader(largeData))
	if err != nil {
		t.Fatalf("Compress() error = %v", err)
	}

	decompressed, err := compressor.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress() error = %v", err)
	}

	var output bytes.Buffer
	_, err = io.Copy(&output, decompressed)
	if err != nil {
		t.Fatalf("Reading decompressed data error = %v", err)
	}

	if output.String() != largeData {
		t.Errorf("Large data decompression failed. Sizes: expected %d, got %d", len(largeData), output.Len())
	}
}

func TestNewS2Compressor(t *testing.T) {
	compressor := NewS2Compressor()
	if compressor == nil {
		t.Fatal("NewS2Compressor() returned nil")
	}

	// Test that it implements the expected interface
	var _ interface {
		Compress(io.Reader) (io.Reader, error)
		Decompress(io.Reader) (io.Reader, error)
	} = compressor
}

func BenchmarkS2Compressor_Compress(b *testing.B) {
	compressor := NewS2Compressor()
	data := strings.Repeat("test data with some content ", 100)
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		reader := strings.NewReader(data)
		_, err := compressor.Compress(reader)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkS2Compressor_Decompress(b *testing.B) {
	compressor := NewS2Compressor()
	data := strings.Repeat("test data with some content ", 100)
	
	// Pre-compress the data
	compressed, err := compressor.Compress(strings.NewReader(data))
	if err != nil {
		b.Fatal(err)
	}
	
	// Read compressed data into buffer
	var compressedBuf bytes.Buffer
	_, err = io.Copy(&compressedBuf, compressed)
	if err != nil {
		b.Fatal(err)
	}
	compressedBytes := compressedBuf.Bytes()
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(compressedBytes)
		decompressed, err := compressor.Decompress(reader)
		if err != nil {
			b.Fatal(err)
		}
		// Read all decompressed data
		io.Copy(io.Discard, decompressed)
	}
}

func BenchmarkS2Compressor_RoundTrip(b *testing.B) {
	compressor := NewS2Compressor()
	data := strings.Repeat("test data with some content ", 100)
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		// Compress
		compressed, err := compressor.Compress(strings.NewReader(data))
		if err != nil {
			b.Fatal(err)
		}
		
		// Decompress
		decompressed, err := compressor.Decompress(compressed)
		if err != nil {
			b.Fatal(err)
		}
		
		// Read all data
		io.Copy(io.Discard, decompressed)
	}
}
