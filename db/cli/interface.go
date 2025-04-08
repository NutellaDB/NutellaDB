package cli

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"db/database"
	"strings"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

// Root command for the CLI
var rootCmd = &cobra.Command{
	Use:   "dbcli",
	Short: "CLI for managing the database",
	Long:  "A Command Line Interface (CLI) for managing collections and data in the custom database application.",
}

// Execute runs the root command
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error executing CLI: %v\n", err)
		os.Exit(1)
	}
}

// Command to create a new database
var createDBCmd = &cobra.Command{
	Use:   "create-db",
	Short: "Create a new database",
	Run: func(cmd *cobra.Command, args []string) {
		dbUUID, err := uuid.NewRandom()
		if err != nil {
			log.Fatalf("failed to generate uuid: %v", err)
		}
		dbSuffix := strings.Split(dbUUID.String(), "-")[0]
		dbID := fmt.Sprintf("db_%s", dbSuffix)
		fmt.Println("Database ID:", dbID)

		basePath := filepath.Join(".", "files", dbID)

		os.RemoveAll(basePath)

		db, err := database.NewDatabase(basePath, dbID)
		if err != nil {
			log.Fatalf("Error creating database: %v", err)
		}

		if err := db.Close(); err != nil {
			log.Fatalf("Error closing database: %v", err)
		}

		fmt.Println("Database created successfully!")
	},
}

// Command to create a collection in a database
var createCollectionCmd = &cobra.Command{
	Use:   "create-collection [dbID] [name] [order]",
	Short: "Create a new collection in the specified database",
	Args:  cobra.ExactArgs(3),
	Run: func(cmd *cobra.Command, args []string) {
		dbID := args[0]
		name := args[1]
		orderStr := args[2]

		order, err := strconv.Atoi(orderStr)
		if err != nil || order < 3 {
			log.Fatalf("Invalid order value '%s'. Order must be an integer >= 3.", orderStr)
		}

		basePath := filepath.Join(".", "files", dbID)

		db, err := database.LoadDatabase(basePath)
		if err != nil {
			log.Fatalf("Error loading database: %v", err)
		}
		defer db.Close()

		if err := db.CreateCollection(name, order); err != nil {
			log.Fatalf("Error creating collection: %v", err)
		}

		fmt.Printf("Collection '%s' created successfully in database '%s'.\n", name, dbID)
	},
}

// Command to insert a key-value pair into a collection
var insertCmd = &cobra.Command{
	Use:   "insert [dbID] [collection] [key] [value]",
	Short: "Insert a key-value pair into a collection in the specified database",
	Long:  "This command inserts a key-value pair into the specified collection within the given database.",
	Args:  cobra.ExactArgs(4),
	Run: func(cmd *cobra.Command, args []string) {
		dbID := args[0]
        collName := args[1]
        key := args[2]
        value := args[3]

        basePath := filepath.Join(".", "files", dbID)

        db, err := database.LoadDatabase(basePath)
        if err != nil {
            log.Fatalf("Error loading database '%s': %v", dbID, err)
        }
        defer db.Close()

        coll, err := db.GetCollection(collName)
        if err != nil {
            log.Fatalf("Error getting collection '%s': %v", collName, err)
        }

        coll.InsertKV(key, value)

        fmt.Printf("Inserted key '%s' with value '%s' into collection '%s' in database '%s'.\n", key, value, collName, dbID)
    },
}

// Command to find a key in a collection
var findKeyCmd = &cobra.Command{
    Use:   "find [dbID] [collection] [key]",
    Short: "Find a key in a collection",
    Args:  cobra.ExactArgs(3),
    Run: func(cmd *cobra.Command, args []string) {
        dbID := args[0]
        collName := args[1]
        key := args[2]

        basePath := filepath.Join(".", "files", dbID)

        db, err := database.LoadDatabase(basePath)
        if err != nil {
            log.Fatalf("Error loading database '%s': %v", dbID, err)
        }
        defer db.Close()

        coll, err := db.GetCollection(collName)
        if err != nil {
            log.Fatalf("Error getting collection '%s': %v", collName, err)
        }

        coll.FindKey(key) // FindKey is called directly on the B-tree
    },
}

// Command to update a key-value pair in a collection
var updateCmd = &cobra.Command{
    Use:   "update [dbID] [collection] [key] [new_value]",
    Short: "Update the value of an existing key in a collection",
    Args:  cobra.ExactArgs(4),
    Run: func(cmd *cobra.Command, args []string) {
        dbID := args[0]
        collName := args[1]
        key := args[2]
        newValue := args[3]

        basePath := filepath.Join(".", "files", dbID)

        db, err := database.LoadDatabase(basePath)
        if err != nil {
            log.Fatalf("Error loading database '%s': %v", dbID, err)
        }
        defer db.Close()

        coll, err := db.GetCollection(collName)
        if err != nil {
            log.Fatalf("Error getting collection '%s': %v", collName, err)
        }

        coll.UpdateKV(key, newValue) // UpdateKV is called directly on the B-tree
    },
}

// Command to delete a key from a collection
var deleteCmd = &cobra.Command{
    Use:   "delete [dbID] [collection] [key]",
    Short: "Delete a key from a collection",
    Args:  cobra.ExactArgs(3),
    Run: func(cmd *cobra.Command, args []string) {
        dbID := args[0]
        collName := args[1]
        key := args[2]

        basePath := filepath.Join(".", "files", dbID)

        db, err := database.LoadDatabase(basePath)
        if err != nil {
            log.Fatalf("Error loading database '%s': %v", dbID, err)
        }
        defer db.Close()

        coll, err := db.GetCollection(collName)
        if err != nil {
            log.Fatalf("Error getting collection '%s': %v", collName, err)
        }

        coll.DeleteKey(key) // DeleteKey is called directly on the B-tree
    },
}

func init() {
	rootCmd.AddCommand(createDBCmd)
	rootCmd.AddCommand(createCollectionCmd)
	rootCmd.AddCommand(insertCmd)
	rootCmd.AddCommand(findKeyCmd) 
	rootCmd.AddCommand(updateCmd)  
	rootCmd.AddCommand(deleteCmd)  
}
