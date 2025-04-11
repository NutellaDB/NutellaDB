package database_test

import (
	"db/database"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestBenchmarkOperations performs benchmarking for database operations
func TestBenchmarkOperations(t *testing.T) {
	// Setup test environment
	dbID := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	dbPath := filepath.Join(".", "files", dbID)
	defer os.RemoveAll(dbPath) // Clean up after test

	// Create database
	db, err := database.NewDatabase(dbPath, dbID)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create collection
	collectionName := "benchmark_collection"
	btreeOrder := 8 // Adjust as needed
	err = db.CreateCollection(collectionName, btreeOrder)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// Get collection
	collection, err := db.GetCollection(collectionName)
	if err != nil {
		t.Fatalf("Failed to get collection: %v", err)
	}

	// Run benchmarks
	benchmarkInsert(t, collection, 1000)
	benchmarkFind(t, collection, 1000)
	benchmarkUpdate(t, collection, 1000)
	// benchmarkDelete(t, collection, 40)
}

// benchmarkInsert tests insertion performance
func benchmarkInsert(t *testing.T, collection *database.Collection, count int) {
	t.Logf("Running Insert benchmark with %d operations", count)

	// Generate keys and values
	keys := make([]string, count)
	values := make([]string, count)
	for i := 0; i < count; i++ {
		keys[i] = fmt.Sprintf("key_%d", i)
		values[i] = fmt.Sprintf("value_%d", i)
	}

	// Benchmark insert operations
	start := time.Now()
	for i := 0; i < count; i++ {
		collection.InsertKV(keys[i], values[i])
	}
	duration := time.Since(start)

	// Validate data
	for i := 0; i < count; i++ {
		value, found := collection.FindKey(keys[i])
		if !found {
			t.Errorf("Validation failed: Key %s not found after insert", keys[i])
			continue
		}
		if value != values[i] {
			t.Errorf("Validation failed: Key %s has value %v, expected %s", keys[i], value, values[i])
		}
	}

	avgTime := float64(duration.Microseconds()) / float64(count)
	t.Logf("Insert benchmark completed: %d operations in %v (avg %.2f µs per operation)",
		count, duration, avgTime)
}

// benchmarkFind tests find performance
func benchmarkFind(t *testing.T, collection *database.Collection, count int) {
	t.Logf("Running Find benchmark with %d operations", count)

	// Generate random sequence for accessing keys
	indices := rand.Perm(count)

	// Benchmark find operations
	start := time.Now()
	successCount := 0
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key_%d", indices[i])
		expectedValue := fmt.Sprintf("value_%d", indices[i])
		value, found := collection.FindKey(key)

		if found {
			successCount++
			if value != expectedValue {
				t.Errorf("Found incorrect value for key %s: got %v, expected %s", key, value, expectedValue)
			}
		}
	}
	duration := time.Since(start)

	avgTime := float64(duration.Microseconds()) / float64(count)
	t.Logf("Find benchmark completed: %d operations in %v (avg %.2f µs per operation)",
		count, duration, avgTime)
	t.Logf("Find success rate: %d/%d (%.2f%%)",
		successCount, count, float64(successCount)*100/float64(count))
}

// benchmarkUpdate tests update performance
func benchmarkUpdate(t *testing.T, collection *database.Collection, count int) {
	t.Logf("Running Update benchmark with %d operations", count)

	// Generate updated values
	updatedValues := make([]string, count)
	for i := 0; i < count; i++ {
		updatedValues[i] = fmt.Sprintf("updated_value_%d", i)
	}

	// Benchmark update operations
	start := time.Now()
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key_%d", i)
		collection.UpdateKV(key, updatedValues[i])
	}
	duration := time.Since(start)

	// Validate updates
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key_%d", i)
		value, found := collection.FindKey(key)
		if !found {
			t.Errorf("Validation failed: Key %s not found after update", key)
			continue
		}
		if value != updatedValues[i] {
			t.Errorf("Validation failed: Key %s has value %v, expected %s", key, value, updatedValues[i])
		}
	}

	avgTime := float64(duration.Microseconds()) / float64(count)
	t.Logf("Update benchmark completed: %d operations in %v (avg %.2f µs per operation)",
		count, duration, avgTime)
}

// benchmarkDelete tests delete performance
func benchmarkDelete(t *testing.T, collection *database.Collection, count int) {
	t.Logf("Running Delete benchmark with %d operations", count)

	// Benchmark delete operations
	start := time.Now()
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key_%d", i)
		collection.DeleteKey(key)
	}
	duration := time.Since(start)

	// Validate deletions
	deletionSuccessCount := 0
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key_%d", i)
		_, found := collection.FindKey(key)
		if !found {
			deletionSuccessCount++
		} else {
			t.Errorf("Validation failed: Key %s still exists after deletion", key)
		}
	}

	avgTime := float64(duration.Microseconds()) / float64(count)
	t.Logf("Delete benchmark completed: %d operations in %v (avg %.2f µs per operation)",
		count, duration, avgTime)
	t.Logf("Delete success rate: %d/%d (%.2f%%)",
		deletionSuccessCount, count, float64(deletionSuccessCount)*100/float64(count))
}

// TestBenchmarkWithRandomAccess simulates real-world mixed operations
func TestBenchmarkWithRandomAccess(t *testing.T) {
	// Setup test environment
	dbID := fmt.Sprintf("test_db_random_%d", time.Now().UnixNano())
	dbPath := filepath.Join(".", "files", dbID)
	defer os.RemoveAll(dbPath) // Clean up after test

	// Create database
	db, err := database.NewDatabase(dbPath, dbID)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create collection
	collectionName := "random_benchmark"
	btreeOrder := 8 // Adjust as needed
	err = db.CreateCollection(collectionName, btreeOrder)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// Get collection
	collection, err := db.GetCollection(collectionName)
	if err != nil {
		t.Fatalf("Failed to get collection: %v", err)
	}

	// Run mixed operation benchmark
	benchmarkMixedOperations(t, collection, 1000)
}

// benchmarkMixedOperations tests random mix of operations
func benchmarkMixedOperations(t *testing.T, collection *database.Collection, count int) {
	t.Logf("Running Mixed Operations benchmark with %d total operations", count)

	// Track which keys exist for validation
	keyExists := make(map[string]bool)
	keyValues := make(map[string]string)

	// Operation types
	const (
		opInsert = iota
		opFind
		opUpdate
		opDelete
		opCount
	)

	operationCounts := make([]int, opCount)
	operationTimes := make([]time.Duration, opCount)

	rand.Seed(time.Now().UnixNano())

	start := time.Now()
	for i := 0; i < count; i++ {
		// Generate a random key between 0 and count*2 to create collisions
		keyIdx := rand.Intn(count * 2)
		key := fmt.Sprintf("random_key_%d", keyIdx)

		// Choose a random operation with different weights
		op := rand.Intn(100)
		// 40% inserts, 40% finds, 15% updates, 5% deletes
		var operation int
		if op < 40 {
			operation = opInsert
		} else if op < 80 {
			operation = opFind
		} else if op < 95 {
			operation = opUpdate
		} else {
			operation = opDelete
		}

		opStart := time.Now()

		switch operation {
		case opInsert:
			value := fmt.Sprintf("random_value_%d", i)
			collection.InsertKV(key, value)
			keyExists[key] = true
			keyValues[key] = value
			operationCounts[opInsert]++

		case opFind:
			value, found := collection.FindKey(key)
			if found && keyExists[key] {
				// Validate value if we know it exists
				if value != keyValues[key] {
					t.Errorf("Find validation failed: Key %s has value %v, expected %s",
						key, value, keyValues[key])
				}
			} else if found && !keyExists[key] {
				t.Errorf("Found key %s that should not exist", key)
			}
			operationCounts[opFind]++

		case opUpdate:
			if keyExists[key] {
				newValue := fmt.Sprintf("updated_value_%d", i)
				collection.UpdateKV(key, newValue)
				keyValues[key] = newValue
			}
			operationCounts[opUpdate]++

		case opDelete:
			if keyExists[key] {
				collection.DeleteKey(key)
				delete(keyExists, key)
				delete(keyValues, key)
			}
			operationCounts[opDelete]++
		}

		operationTimes[operation] += time.Since(opStart)
	}
	totalDuration := time.Since(start)

	// Report statistics
	t.Logf("Mixed Operations benchmark completed: %d operations in %v", count, totalDuration)
	t.Logf("Average time per operation: %.2f µs", float64(totalDuration.Microseconds())/float64(count))

	operationNames := []string{"Insert", "Find", "Update", "Delete"}
	for i := 0; i < opCount; i++ {
		if operationCounts[i] > 0 {
			avgTime := float64(operationTimes[i].Microseconds()) / float64(operationCounts[i])
			t.Logf("  %s: %d operations, avg %.2f µs per operation",
				operationNames[i], operationCounts[i], avgTime)
		}
	}

	// Validate final state - count remaining keys
	remainingKeys := len(keyExists)
	t.Logf("Final state: %d keys in database", remainingKeys)

	// Sample validation of a few random keys
	validationCount := 10
	if remainingKeys < validationCount {
		validationCount = remainingKeys
	}

	if validationCount > 0 {
		t.Log("Validating a sample of keys...")
		validated := 0
		for key, expectedValue := range keyValues {
			if validated >= validationCount {
				break
			}
			value, found := collection.FindKey(key)
			if !found {
				t.Errorf("Final validation failed: Key %s not found but should exist", key)
			} else if value != expectedValue {
				t.Errorf("Final validation failed: Key %s has value %v, expected %s",
					key, value, expectedValue)
			}
			validated++
		}
	}
}
