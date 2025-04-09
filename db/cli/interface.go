package cli

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"db/database"
	"strings"
    "bytes"
	"compress/zlib"
	"crypto/sha1"
	"encoding/hex"
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

var handleInitCmd = &cobra.Command{
	Use:   "init [dbID]",
	Short: "Initialize a new git directory",
	Long:  "This command initializes a new git directory in the specified database folder.",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		dbID := args[0]
		basePath := filepath.Join(".", "files", dbID)

		if err := os.MkdirAll(basePath, 0755); err != nil {
			fmt.Fprintf(os.Stderr, "Error creating base directory: %s\n", err)
			return
		}

		// Initialize git repository in the provided basePath
		handleInit(basePath)
	},
}

func handleInit(basePath string) {
	// Create the .nut folder within the basePath
	gitDir := filepath.Join(basePath, ".nut")
	dirs := []string{
		gitDir,
		filepath.Join(gitDir, "objects"),
		filepath.Join(gitDir, "refs"),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			fmt.Fprintf(os.Stderr, "Error creating directory %s: %s\n", dir, err)
		}
	}

	// Write the HEAD file within the .nut directory.
	headFileContents := []byte("ref: refs/heads/main\n")
	headFilePath := filepath.Join(gitDir, "HEAD")
	if err := os.WriteFile(headFilePath, headFileContents, 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing HEAD file: %s\n", err)
	}
	fmt.Printf("Initialized git directory at %s\n", gitDir)
}




// commitMessage will hold the commit message from the "-m" flag.
var commitMessage string

// handleCommitAllCmd represents the commit-all command.
// Usage: go run . commit-all <dbID> -m "message here"
var handleCommitAllCmd = &cobra.Command{
	Use:   "commit-all <dbID>",
	Short: "Recursively hash files, create a tree and commit object for the given db",
	Long: `This command does the following:
  1. Uses the provided dbID to locate the repository at "./files/<dbID>".
  2. Loads ignore patterns from .nutignore.
  3. Recursively hashes all files in the repository (ignoring .nut and matching ignore patterns).
  4. Writes a tree object for the entire directory structure.
  5. Creates and stores a commit object with the provided commit message.`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		dbID := args[0]
		basePath := filepath.Join(".", "files", dbID)
		repoGitDir := filepath.Join(basePath, ".nut")

		// Verify that the repository exists.
		if _, err := os.Stat(repoGitDir); os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "Error: repository not found at %s. Please run 'init' first.\n", basePath)
			os.Exit(1)
		}

		// Change current working directory to the repository base.
		if err := os.Chdir(basePath); err != nil {
			fmt.Fprintf(os.Stderr, "Error changing directory to %s: %s\n", basePath, err)
			os.Exit(1)
		}

		if commitMessage == "" {
			fmt.Fprintf(os.Stderr, "Error: commit message cannot be empty. Usage: commit-all <dbID> -m \"<message>\"\n")
			os.Exit(1)
		}

		// Load ignore patterns from .nutignore (or .nutignore as per your naming convention).
		ignores, err := loadGitignore()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading .nutignore: %s\n", err)
			os.Exit(1)
		}

		// Create tree recursively from the repository directory.
		treeSha, err := writeTreeRecursive(".", ".", ignores)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error writing tree: %s\n", err)
			os.Exit(1)
		}

		// Create and store commit object referencing the generated tree.
		createAndStoreCommit(treeSha, commitMessage)
	},
}



// createAndStoreCommit creates a commit object with the given tree SHA and commit message.
func createAndStoreCommit(treeSha, message string) {
	commitContent := fmt.Sprintf("tree %s\n\n%s\n", treeSha, message)
	header := fmt.Sprintf("commit %d\u0000", len(commitContent))
	store := append([]byte(header), []byte(commitContent)...)
	hash := sha1.Sum(store)
	sha := fmt.Sprintf("%x", hash)

	// Build path to store the commit object under .nut/objects.
	dir := sha[:2]
	name := sha[2:]
	path := filepath.Join(".nut", "objects", dir, name)

	if err := os.MkdirAll(filepath.Join(".nut", "objects", dir), 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating commit directory: %s\n", err)
		os.Exit(1)
	}

	var buf bytes.Buffer
	w := zlib.NewWriter(&buf)
	_, _ = w.Write(store)
	w.Close()

	if err := os.WriteFile(path, buf.Bytes(), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing commit: %s\n", err)
		os.Exit(1)
	}

	fmt.Println(sha)
}

// loadGitignore reads the .nutignore (or .nutignore) file and returns the list of ignore patterns.
func loadGitignore() ([]string, error) {
	data, err := os.ReadFile(".nutignore")
	if os.IsNotExist(err) {
		return []string{}, nil
	}
	if err != nil {
		return nil, err
	}

	lines := strings.Split(string(data), "\n")
	var patterns []string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		patterns = append(patterns, line)
	}
	return patterns, nil
}

// writeTreeRecursive creates a tree object for the directory (relative to repo root).
// 'root' is the repository root (".") and 'dir' is the current directory relative to root.
func writeTreeRecursive(root, dir string, ignores []string) (string, error) {
	var entries []byte

	fullDir := filepath.Join(root, dir)
	files, err := os.ReadDir(fullDir)
	if err != nil {
		return "", err
	}

	for _, f := range files {
		// Ignore the .nut folder.
		if f.Name() == ".nut" {
			continue
		}
		// Compute relative path from repo root.
		relPath := f.Name()
		if dir != "." {
			relPath = filepath.Join(dir, f.Name())
		}
		// Skip if path matches any ignore pattern.
		if shouldIgnore(relPath, ignores) {
			continue
		}

		var mode string
		var sha string
		fullPath := filepath.Join(fullDir, f.Name())
		if f.IsDir() {
			mode = "40000"
			sha, err = writeTreeRecursive(root, relPath, ignores)
			if err != nil {
				return "", err
			}
		} else {
			mode = "100644"
			sha, err = hashAndWriteBlob(fullPath)
			if err != nil {
				return "", err
			}
		}

		// Create tree entry: "<mode> <filename>\0<sha>"
		entry := fmt.Sprintf("%s %s", mode, f.Name())
		entryBytes := []byte(entry)
		entryBytes = append(entryBytes, 0)
		shaRaw, _ := hex.DecodeString(sha)
		entryBytes = append(entryBytes, shaRaw...)
		entries = append(entries, entryBytes...)
	}

	header := fmt.Sprintf("tree %d\u0000", len(entries))
	store := append([]byte(header), entries...)
	hash := sha1.Sum(store)
	shaStr := fmt.Sprintf("%x", hash)
	dirName := shaStr[:2]
	fileName := shaStr[2:]
	objPath := filepath.Join(".nut", "objects", dirName, fileName)

	if err := os.MkdirAll(filepath.Dir(objPath), 0755); err != nil {
		return "", err
	}
	var buf bytes.Buffer
	w := zlib.NewWriter(&buf)
	_, _ = w.Write(store)
	w.Close()
	if err := os.WriteFile(objPath, buf.Bytes(), 0644); err != nil {
		return "", err
	}
	return shaStr, nil
}

// shouldIgnore checks if the given relative path matches any of the ignore patterns.
func shouldIgnore(relPath string, patterns []string) bool {
	for _, pattern := range patterns {
		if matched, _ := filepath.Match(pattern, relPath); matched {
			return true
		}
		if strings.Contains(relPath, pattern) {
			return true
		}
	}
	return false
}

// hashAndWriteBlob creates a blob object from the given file and returns its SHA.
func hashAndWriteBlob(filename string) (string, error) {
	content, err := os.ReadFile(filename)
	if err != nil {
		return "", err
	}
	header := fmt.Sprintf("blob %d\u0000", len(content))
	store := append([]byte(header), content...)
	hash := sha1.Sum(store)
	sha := fmt.Sprintf("%x", hash)
	dir := sha[:2]
	name := sha[2:]
	objPath := filepath.Join(".nut", "objects", dir, name)
	if _, err := os.Stat(objPath); os.IsNotExist(err) {
		if err := os.MkdirAll(filepath.Join(".nut", "objects", dir), 0755); err != nil {
			return "", err
		}
		var buf bytes.Buffer
		w := zlib.NewWriter(&buf)
		_, _ = w.Write(store)
		w.Close()
		if err := os.WriteFile(objPath, buf.Bytes(), 0644); err != nil {
			return "", err
		}
	}
	return sha, nil
}




func init() {
	rootCmd.AddCommand(createDBCmd)
	rootCmd.AddCommand(createCollectionCmd)
	rootCmd.AddCommand(insertCmd)
	rootCmd.AddCommand(findKeyCmd) 
	rootCmd.AddCommand(updateCmd)  
	rootCmd.AddCommand(deleteCmd) 
    rootCmd.AddCommand(handleInitCmd) 
    rootCmd.AddCommand(handleCommitAllCmd)
	handleCommitAllCmd.Flags().StringVarP(&commitMessage, "message", "m", "", "Commit message")
}
