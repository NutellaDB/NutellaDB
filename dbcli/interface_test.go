package dbcli

import (
	"bytes"
	"compress/zlib"
	"crypto/rand"
	"crypto/sha1"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"
)

// TestComputeDelta verifies that computing a delta works correctly
func TestComputeDelta(t *testing.T) {
	base := []byte("This is some base content that we'll modify slightly to test delta generation.")
	target := []byte("This is some base content that we've modified slightly to test delta generation!")

	delta := computeDelta(base, target)

	// Verify we can reconstruct the target from base + delta
	reconstructed, err := applyDelta(base, delta)
	if err != nil {
		t.Fatalf("Failed to apply delta: %v", err)
	}

	if !bytes.Equal(target, reconstructed) {
		t.Errorf("Reconstruction failed. Got %s, want %s", reconstructed, target)
	}
}

// BenchmarkComputeDelta measures performance of delta computation
func BenchmarkComputeDelta(b *testing.B) {
	// Create test data with varying sizes and similarities
	testCases := []struct {
		name       string
		baseSize   int
		targetSize int
		similarity float32 // 0.0-1.0 where 1.0 is identical
	}{
		{"Small_Similar", 100, 110, 0.9},
		{"Small_Different", 100, 120, 0.5},
		// {"Medium_Similar", 10000, 10200, 0.95},
		// {"Medium_Different", 10000, 10500, 0.6},
		// {"Large_Similar", 1000000, 1010000, 0.98},
		// {"Large_Different", 1000000, 1050000, 0.7},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			base := generateTestData(tc.baseSize)
			target := generateSimilarData(base, tc.similarity, tc.targetSize)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				delta := computeDelta(base, target)
				// Verify correctness on first iteration
				if i == 0 {
					result, err := applyDelta(base, delta)
					if err != nil {
						b.Fatalf("Delta application failed: %v", err)
					}
					if !bytes.Equal(result, target) {
						b.Fatalf("Delta reconstruction failed")
					}

					// Report compression ratio
					compressionRatio := float64(len(delta)) / float64(len(target))
					b.Logf("Delta size: %d bytes (%.2f%% of target)", len(delta), compressionRatio*100)
				}
			}
		})
	}
}

// BenchmarkApplyDelta measures delta application performance
func BenchmarkApplyDelta(b *testing.B) {
	testCases := []struct {
		name       string
		baseSize   int
		targetSize int
		similarity float32
	}{
		{"Small", 100, 110, 0.9},
		// {"Medium", 10000, 10200, 0.95},
		// {"Large", 1000000, 1010000, 0.98},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			base := generateTestData(tc.baseSize)
			target := generateSimilarData(base, tc.similarity, tc.targetSize)
			delta := computeDelta(base, target)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := applyDelta(base, delta)
				if err != nil {
					b.Fatalf("Delta application failed: %v", err)
				}
				if i == 0 && !bytes.Equal(result, target) {
					b.Fatalf("Delta reconstruction failed")
				}
			}
		})
	}
}

// BenchmarkDeltaOperationComputation tests the performance of computing delta operations
func BenchmarkDeltaOperationComputation(b *testing.B) {
	testCases := []struct {
		name       string
		baseSize   int
		targetSize int
		similarity float32
	}{
		{"Small_Similar", 100, 110, 0.9},
		// {"Medium_Similar", 10000, 10200, 0.95},
		// {"Large_Similar", 1000000, 1010000, 0.98},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			base := generateTestData(tc.baseSize)
			target := generateSimilarData(base, tc.similarity, tc.targetSize)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ops := computeDeltaOperations(base, target)
				if i == 0 {
					b.Logf("Generated %d operations", len(ops))

					// Count copy vs insert operations
					copyOps := 0
					insertBytes := 0
					for _, op := range ops {
						if op.IsCopy {
							copyOps++
						} else {
							insertBytes += len(op.Data)
						}
					}
					b.Logf("Copy operations: %d, Insert bytes: %d", copyOps, insertBytes)
				}
			}
		})
	}
}

// BenchmarkFindSimilarObject tests object similarity detection performance
func BenchmarkFindSimilarObject(b *testing.B) {
	// Setup test repository
	tempDir, err := os.MkdirTemp("", "nutella-benchmark")
	if err != nil {
		b.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create object directory structure
	objDir := filepath.Join(tempDir, ".nutella", "objects")
	if err := os.MkdirAll(objDir, 0755); err != nil {
		b.Fatalf("Failed to create objects directory: %v", err)
	}

	// Change to the test directory
	originalDir, err := os.Getwd()
	if err != nil {
		b.Fatalf("Failed to get current directory: %v", err)
	}
	if err := os.Chdir(tempDir); err != nil {
		b.Fatalf("Failed to change directory: %v", err)
	}
	defer os.Chdir(originalDir)

	// Create test objects with varying similarities
	baseBlobSize := 10000
	baseBlob := generateTestData(baseBlobSize)
	testObjects := []struct {
		similarity float32
		size       int
	}{
		{0.99, baseBlobSize}, // Nearly identical
		{0.9, baseBlobSize},  // Very similar
		{0.8, baseBlobSize},  // Somewhat similar
		{0.5, baseBlobSize},  // Half similar
		{0.1, baseBlobSize},  // Mostly different
	}

	// Create test objects in the repository
	objIDs := make([]string, len(testObjects))
	for i, tc := range testObjects {
		content := generateSimilarData(baseBlob, tc.similarity, tc.size)
		objID := createTestObject(content, objDir)
		objIDs[i] = objID
		b.Logf("Created test object %s with similarity %.2f", objID, tc.similarity)
	}

	// Create target blob with high similarity to baseBlob
	targetBlob := generateSimilarData(baseBlob, 0.95, baseBlobSize)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		objID, content := findSimilarObject(targetBlob)
		if i == 0 {
			if objID == "" {
				b.Logf("No similar object found")
			} else {
				sim := calculateSimilarity(content, targetBlob)
				b.Logf("Found object %s with similarity %.4f", objID, sim)
			}
		}
	}
}

// BenchmarkEndToEndObjectStorage tests the entire storage workflow
func BenchmarkEndToEndObjectStorage(b *testing.B) {
	// Setup test repository
	tempDir, err := os.MkdirTemp("", "nutella-benchmark")
	if err != nil {
		b.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create necessary directories
	if err := os.MkdirAll(filepath.Join(tempDir, ".nutella", "objects"), 0755); err != nil {
		b.Fatalf("Failed to create objects directory: %v", err)
	}

	// Change to the test directory
	originalDir, err := os.Getwd()
	if err != nil {
		b.Fatalf("Failed to get current directory: %v", err)
	}
	if err := os.Chdir(tempDir); err != nil {
		b.Fatalf("Failed to change directory: %v", err)
	}
	defer os.Chdir(originalDir)

	// Create base files to establish repository state
	baseContentSizes := []int{1000, 10000, 100000}
	baseFiles := make([]string, len(baseContentSizes))

	for i, size := range baseContentSizes {
		content := generateTestData(size)
		filePath := filepath.Join(tempDir, fmt.Sprintf("base_file_%d.txt", i))
		if err := os.WriteFile(filePath, content, 0644); err != nil {
			b.Fatalf("Failed to write base file: %v", err)
		}
		baseFiles[i] = filePath

		// Store the file as an object
		_, err := hashAndWriteBlob(filePath)
		if err != nil {
			b.Fatalf("Failed to store base file: %v", err)
		}
	}

	// Now prepare test files with varying similarities to the base files
	testCases := []struct {
		name        string
		similarity  float32
		baseFileIdx int
		sizeChange  int // bytes to add/remove
	}{
		// {"High_Similarity_Small", 0.95, 0, 50},
		// {"Medium_Similarity_Small", 0.8, 0, 200},
		// {"High_Similarity_Medium", 0.95, 1, 500},
		// {"Medium_Similarity_Medium", 0.8, 1, 1000},
		// {"High_Similarity_Large", 0.95, 2, 5000},
		// {"Medium_Similarity_Large", 0.8, 2, 10000},
	}

	b.ResetTimer()

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			// Read the base file
			baseContent, err := os.ReadFile(baseFiles[tc.baseFileIdx])
			if err != nil {
				b.Fatalf("Failed to read base file: %v", err)
			}

			// Create the test file with desired similarity
			testContent := generateSimilarData(baseContent, tc.similarity, len(baseContent)+tc.sizeChange)
			testFilePath := filepath.Join(tempDir, fmt.Sprintf("test_%s.txt", tc.name))
			if err := os.WriteFile(testFilePath, testContent, 0644); err != nil {
				b.Fatalf("Failed to write test file: %v", err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Store the file and measure performance
				objID, err := hashAndWriteBlob(testFilePath)
				if err != nil {
					b.Fatalf("Failed to hash and write blob: %v", err)
				}

				// Verify on first iteration
				if i == 0 {
					// Read back and verify
					storedData := readObjectTest(objID)

					// Split the header from the content
					nullIndex := bytes.IndexByte(storedData, 0)
					if nullIndex == -1 {
						b.Fatalf("Invalid object: missing null byte")
					}
					storedContent := storedData[nullIndex+1:]

					if !bytes.Equal(storedContent, testContent) {
						b.Fatalf("Content mismatch: object storage or retrieval failed")
					}

					// Check if it was stored as a delta
					objPath := filepath.Join(".nutella", "objects", objID[:2], objID[2:])
					objData, err := os.ReadFile(objPath)
					if err != nil {
						b.Fatalf("Failed to read stored object: %v", err)
					}

					// Decompress and check the header to see if it's a delta
					r, _ := zlib.NewReader(bytes.NewReader(objData))
					headerBuf := make([]byte, 100) // Should be enough for header
					n, _ := r.Read(headerBuf)
					r.Close()

					isDelta := bytes.HasPrefix(headerBuf[:n], []byte("delta "))
					if isDelta {
						b.Logf("Object stored as delta")
					} else {
						b.Logf("Object stored as full blob")
					}
				}
			}
		})
	}
}

// Helper functions to generate test data

// generateTestData creates random data of the specified size
func generateTestData(size int) []byte {
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		log.Fatalf("Failed to generate random data: %v", err)
	}
	return data
}

// generateSimilarData creates a new byte slice that has the specified similarity
// to the base data and reaches the target size
func generateSimilarData(base []byte, similarity float32, targetSize int) []byte {
	if similarity < 0 || similarity > 1 {
		log.Fatalf("Similarity must be between 0 and 1")
	}

	// Calculate how many bytes to keep unchanged
	keepBytes := int(float32(len(base)) * similarity)
	if keepBytes > targetSize {
		keepBytes = targetSize
	}

	// Calculate bytes to add or remove
	sizeDiff := targetSize - len(base)

	// Create the result starting with the base
	result := make([]byte, len(base))
	copy(result, base)

	// If we need to change data for similarity
	if similarity < 1.0 {
		// Change a portion of the bytes
		changePortion := int(float32(len(base)) * (1 - similarity))
		changeIndices := make([]int, changePortion)

		// Generate random indices to change
		for i := 0; i < changePortion; i++ {
			changeIndices[i] = i // We'll change the first changePortion bytes for simplicity
		}

		// Change those bytes
		for _, idx := range changeIndices {
			if idx < len(result) {
				result[idx] = result[idx] ^ 0xFF // Simple bit flip
			}
		}
	}

	// Adjust size if needed
	if sizeDiff > 0 {
		// Need to add bytes
		additional := make([]byte, sizeDiff)
		if _, err := rand.Read(additional); err != nil {
			log.Fatalf("Failed to generate random data: %v", err)
		}
		result = append(result, additional...)
	} else if sizeDiff < 0 {
		// Need to remove bytes
		result = result[:targetSize]
	}

	return result
}

// createTestObject creates a blob object in the test repository and returns its objID
func createTestObject(content []byte, objDir string) string {
	// Create a blob object
	header := fmt.Sprintf("blob %d\u0000", len(content))
	store := append([]byte(header), content...)
	hash := sha1.Sum(store)
	sha := fmt.Sprintf("%x", hash)

	// Build path to store the blob object
	dir := sha[:2]
	name := sha[2:]
	objPath := filepath.Join(objDir, dir, name)

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Join(objDir, dir), 0755); err != nil {
		log.Fatalf("Error creating object directory: %v", err)
	}

	// Compress and write the object
	var buf bytes.Buffer
	w := zlib.NewWriter(&buf)
	_, _ = w.Write(store)
	w.Close()

	if err := os.WriteFile(objPath, buf.Bytes(), 0644); err != nil {
		log.Fatalf("Error writing blob object: %v", err)
	}

	return sha
}

// Helper function needed for the imports to work
func readObjectTest(sha string) []byte {
	// This function is already defined in the original code
	// We're reimplementing it here for test purposes
	dir, name := sha[:2], sha[2:]
	path := filepath.Join(".nutella", "objects", dir, name)
	data, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("Error reading object file: %v\n", err)
	}

	r, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		log.Fatalf("Error creating zlib reader: %v\n", err)
	}
	defer r.Close()

	decompressedData, err := io.ReadAll(r)
	if err != nil {
		log.Fatalf("Error decompressing data: %v\n", err)
	}

	return decompressedData
}

// Note: This benchmark imports:
// - "bytes"
// - "crypto/rand"
// - "testing"
// - "os"
// - "path/filepath"
// - "crypto/sha1"
// - "fmt"
// - "log"
// - "io"
// - "compress/zlib" (implied in the readObjectTest implementation)
