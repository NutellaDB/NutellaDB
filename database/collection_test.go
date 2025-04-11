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
	benchmarkInsert(t, collection, 10000)
	benchmarkFind(t, collection, 10000)
	benchmarkUpdate(t, collection, 10000)
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
