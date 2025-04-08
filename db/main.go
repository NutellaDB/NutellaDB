package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"db/btree" // Import the btree package

	"github.com/google/uuid"
)

func main() {
	// Clean up any existing database files for this example
	dbUUID, err := uuid.NewRandom()
	dbID := fmt.Sprintf("db_%v", strings.Split(dbUUID.String(), "-")[0])
	if err != nil {
		log.Fatalf("Error in UUID generation %v", err)
	}
	dbPath := filepath.Join(".", "files", dbID)
	os.RemoveAll(dbPath)

	// Create a new B-tree with order 3 and DB ID "example_db"
	bt, err := btree.NewBTree(3, dbID)
	if err != nil {
		log.Fatalf("Failed to create B-tree: %v", err)
	}

	// Insert some key-value pairs
	fmt.Println("Inserting key-value pairs...")
	bt.InsertKV("apple", "red fruit")
	bt.InsertKV("banana", "yellow fruit")
	bt.InsertKV("cherry", "small red fruit")
	bt.InsertKV("date", "sweet dried fruit")
	bt.InsertKV("elderberry", "small black fruit")
	bt.InsertKV("fig", "sweet fruit")
	bt.InsertKV("grape", "small juicy fruit")
	bt.InsertKV("grape", "small juicy fruit")

	// Find some keys
	fmt.Println("\nFinding values...")
	bt.FindKey("apple")
	bt.FindKey("banana")
	bt.FindKey("cherry")
	bt.FindKey("orange") // This key doesn't exist

	// Update some keys
	fmt.Println("\nUpdating values...")
	bt.UpdateKV("apple", "crunchy red fruit")
	bt.UpdateKV("fig", "purple sweet fruit")
	bt.UpdateKV("orange", "citrus fruit") // This key doesn't exist yet

	// Find the updated keys
	fmt.Println("\nFinding updated values...")
	bt.FindKey("apple")
	bt.FindKey("fig")
	bt.FindKey("orange")

	// Delete some keys
	fmt.Println("\nDeleting keys...")
	bt.DeleteKey("banana")
	bt.DeleteKey("elderberry")
	bt.DeleteKey("watermelon") // This key doesn't exist

	// Check if the deleted keys exist
	fmt.Println("\nChecking deleted keys...")
	bt.FindKey("banana")
	bt.FindKey("elderberry")

	// Insert the deleted keys again
	fmt.Println("\nRe-inserting deleted keys...")
	bt.InsertKV("banana", "yellow curved fruit")
	bt.InsertKV("elderberry", "small dark berry")

	// Find the re-inserted keys
	fmt.Println("\nFinding re-inserted keys...")
	bt.FindKey("banana")
	bt.FindKey("elderberry")

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
	bt.FindKey("apple")
	bt.FindKey("banana")
	bt.FindKey("cherry")
	bt.FindKey("date")

	fmt.Println("\nCheck Complete")
}
