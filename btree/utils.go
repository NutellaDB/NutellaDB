package btree

import (
	"fmt"
	"log"
)

func (bt *BTree) InsertKV(key string, value interface{}) {
	err := bt.Insert(key, value)
	if err != nil {
		log.Fatalf("Failed to insert key %s: %v", key, err)
	}
	// fmt.Printf("Inserted key: %s, value: %v\n", key, value)
}

func (bt *BTree) FindKey(key string) {
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

func (bt *BTree) UpdateKV(key string, value interface{}) {
	updated, err := bt.Update(key, value)
	if err != nil {
		log.Fatalf("Failed to update key %s: %v", key, err)
	}
	if updated {
		// fmt.Printf("Updated key: %s, new value: %v\n", key, value)
	} else {
		fmt.Printf("Key not found for update, inserting instead: %s\n", key)
		err = bt.Insert(key, value)
		if err != nil {
			log.Fatalf("Failed to insert key %s: %v", key, err)
		}
	}
}

func (bt *BTree) DeleteKey(key string) {
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
