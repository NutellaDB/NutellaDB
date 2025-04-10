package main

import (
	"db/cache"
	"db/database"
	cli "db/dbcli"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
)

func check() {
	dbUUID, err := uuid.NewRandom()
	if err != nil {
		log.Fatalf("failed to generate uuid: %v", err)
	}
	dbSuffix := strings.Split(dbUUID.String(), "-")[0] // grab first segment
	dbID := fmt.Sprintf("db_%s", dbSuffix)
	fmt.Println("Database ID:", dbID)

	basePath := filepath.Join(".", "files", dbID)

	// cleanup old stuff first
	os.RemoveAll(basePath)

	db, err := database.NewDatabase(basePath, dbID)
	if err != nil {
		log.Fatalf("Error creating database: %v", err)
	}

	// set up fruits collection
	if err := db.CreateCollection("fruits", 3); err != nil {
		log.Fatalf("Error creating fruits collection: %v", err)
	}
	fruits, _ := db.GetCollection("fruits")
	fruits.InsertKV("apple", "red fruit")
	fruits.InsertKV("banana", "yellow fruit")
	fruits.InsertKV("cherry", "small red fruit")
	fruits.InsertKV("date", "sweet brown fruit")
	fruits.InsertKV("elderberry", "dark purple berries")
	fruits.InsertKV("fig", "sweet purple fruit")
	fruits.InsertKV("grape", "small round fruit")
	fruits.InsertKV("honeydew", "green melon")
	fruits.InsertKV("kiwi", "fuzzy brown fruit")
	fruits.InsertKV("lemon", "sour yellow citrus")
	fruits.InsertKV("mango", "tropical orange fruit")
	fruits.InsertKV("nectarine", "smooth peach")

	// set up plants collection
	if err := db.CreateCollection("plants", 3); err != nil {
		log.Fatalf("Error creating plants collection: %v", err)
	}
	plants, _ := db.GetCollection("plants")
	plants.InsertKV("aloe", "succulent plant")
	plants.InsertKV("bamboo", "fast growing grass")
	plants.InsertKV("cactus", "desert plant")
	plants.InsertKV("daisy", "common flower")
	plants.InsertKV("eucalyptus", "tall tree")
	plants.InsertKV("fern", "leafy plant")
	plants.InsertKV("ivy", "climbing plant")
	plants.InsertKV("jasmine", "fragrant flower")
	plants.InsertKV("lavender", "purple flower")
	plants.InsertKV("mint", "aromatic herb")
	plants.InsertKV("oak", "hardwood tree")
	plants.InsertKV("palm", "tropical tree")
	plants.InsertKV("rose", "thorny flower")
	plants.InsertKV("sage", "herb plant")

	// try finding some fruits
	fmt.Println("\n-- Testing fruit lookups across pages --")
	fruits.FindKey("apple")
	fruits.FindKey("grape")
	fruits.FindKey("mango")
	fruits.FindKey("orange") // should fail

	// try finding some plants
	fmt.Println("\n-- Testing plant lookups across pages --")
	plants.FindKey("aloe")
	plants.FindKey("jasmine")
	plants.FindKey("sage")
	plants.FindKey("zinnia") // should fail

	// cleanup
	if err := db.Close(); err != nil {
		log.Fatalf("Error closing database: %v", err)
	}
	fmt.Println("\nDatabase closed successfully.")

	// reopen and verify data persisted
	fmt.Println("\nRe-opening database:", dbID)
	db2, err := database.LoadDatabase(basePath)
	if err != nil {
		log.Fatalf("Error loading database: %v", err)
	}

	// check fruits still there
	fruitsAgain, err := db2.GetCollection("fruits")
	if err != nil {
		log.Fatalf("Error loading fruits collection: %v", err)
	}
	fmt.Println("\n-- Verifying fruits data after reopen --")
	fruitsAgain.FindKey("apple")
	fruitsAgain.FindKey("honeydew")
	fruitsAgain.FindKey("nectarine")

	// check plants still there
	plantsAgain, err := db2.GetCollection("plants")
	if err != nil {
		log.Fatalf("Error loading plants collection: %v", err)
	}
	fmt.Println("\n-- Verifying plants data after reopen --")
	plantsAgain.FindKey("bamboo")
	plantsAgain.FindKey("lavender")
	plantsAgain.FindKey("sage")

	// final cleanup
	if err := db2.Close(); err != nil {
		log.Fatalf("Error closing database again: %v", err)
	}

	fmt.Println("\nAll done!")

	cache.CreateCache(basePath)

	cli.Execute()
}
