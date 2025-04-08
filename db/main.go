package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"db/btree" // Import the btree package
)

func main() {
	// Clean up any existing database files for this example
	dbID := "example_db"
	dbPath := filepath.Join(".", "files", dbID)
	os.RemoveAll(dbPath)

	// Create a new B-tree with order 3 and DB ID "example_db"
	bt, err := btree.NewBTree(3, dbID)
	if err != nil {
		log.Fatalf("Failed to create B-tree: %v", err)
	}

	// Insert some key-value pairs
	fmt.Println("Inserting key-value pairs...")
	insertKV(bt, "apple", "red fruit")
	insertKV(bt, "banana", "yellow fruit")
	insertKV(bt, "cherry", "small red fruit")
	insertKV(bt, "date", "sweet dried fruit")
	insertKV(bt, "elderberry", "small black fruit")
	insertKV(bt, "fig", "sweet fruit")
	insertKV(bt, "grape", "small juicy fruit")

	// Find some keys
	fmt.Println("\nFinding values...")
	findKey(bt, "apple")
	findKey(bt, "banana")
	findKey(bt, "cherry")
	findKey(bt, "orange") // This key doesn't exist

	// Update some keys
	fmt.Println("\nUpdating values...")
	updateKV(bt, "apple", "crunchy red fruit")
	updateKV(bt, "fig", "purple sweet fruit")
	updateKV(bt, "orange", "citrus fruit") // This key doesn't exist yet

	// Find the updated keys
	fmt.Println("\nFinding updated values...")
	findKey(bt, "apple")
	findKey(bt, "fig")
	findKey(bt, "orange")

	// Delete some keys
	fmt.Println("\nDeleting keys...")
	deleteKey(bt, "banana")
	deleteKey(bt, "elderberry")
	deleteKey(bt, "watermelon") // This key doesn't exist

	// Check if the deleted keys exist
	fmt.Println("\nChecking deleted keys...")
	findKey(bt, "banana")
	findKey(bt, "elderberry")

	// Insert the deleted keys again
	fmt.Println("\nRe-inserting deleted keys...")
	insertKV(bt, "banana", "yellow curved fruit")
	insertKV(bt, "elderberry", "small dark berry")

	// Find the re-inserted keys
	fmt.Println("\nFinding re-inserted keys...")
	findKey(bt, "banana")
	findKey(bt, "elderberry")

	// Close the B-tree
	err = bt.Close()
	if err != nil {
		log.Fatalf("Failed to close B-tree: %v", err)
	}
	fmt.Println("\nB-tree closed successfully")

	// Re-open the B-tree
	fmt.Println("\nRe-opening the B-tree...")
	bt, err = btree.LoadBTree(dbID)
	if err != nil {
		log.Fatalf("Failed to load B-tree: %v", err)
	}
	fmt.Println("B-tree loaded successfully")

	// Find some keys to verify the data persisted
	fmt.Println("\nVerifying data after re-opening...")
	findKey(bt, "apple")
	findKey(bt, "banana")
	findKey(bt, "cherry")
	findKey(bt, "date")

	fmt.Println("\nExample complete. Check the files directory to see the human-readable database files.")
}

func insertKV(bt *btree.BTree, key string, value interface{}) {
	err := bt.Insert(key, value)
	if err != nil {
		log.Fatalf("Failed to insert key %s: %v", key, err)
	}
	fmt.Printf("Inserted key: %s, value: %v\n", key, value)
}

func findKey(bt *btree.BTree, key string) {
	value, found, err := bt.Find(key)
	if err != nil {
		log.Fatalf("Failed to find key %s: %v", key, err)
	}
	if found {
		fmt.Printf("Found key: %s, value: %v\n", key, value)
	} else {
		fmt.Printf("Key not found: %s\n", key)
	}
}

func updateKV(bt *btree.BTree, key string, value interface{}) {
	updated, err := bt.Update(key, value)
	if err != nil {
		log.Fatalf("Failed to update key %s: %v", key, err)
	}
	if updated {
		fmt.Printf("Updated key: %s, new value: %v\n", key, value)
	} else {
		fmt.Printf("Key not found for update, inserting instead: %s\n", key)
		err = bt.Insert(key, value)
		if err != nil {
			log.Fatalf("Failed to insert key %s: %v", key, err)
		}
	}
}

func deleteKey(bt *btree.BTree, key string) {
	deleted, err := bt.Delete(key)
	if err != nil {
		log.Fatalf("Failed to delete key %s: %v", key, err)
	}
	if deleted {
		fmt.Printf("Deleted key: %s\n", key)
	} else {
		fmt.Printf("Key not found for deletion: %s\n", key)
	}
}
