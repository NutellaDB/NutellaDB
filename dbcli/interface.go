package dbcli

import (
	"bytes"
	"compress/zlib"
	"crypto/sha1"
	"db/database"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

type PackObject struct {
	Type       int    // 1=commit, 2=tree, 3=blob, 4=delta (ref delta), 7=obj_delta (offset delta)
	Data       []byte // object data or delta instructions
	Size       int    // size of the object
	BaseObjID  string // base object SHA for ref delta
	BaseOffset int64  // base object offset for offset delta
}

// DeltaOperation represents a single delta operation (copy or insert)
type DeltaOperation struct {
	IsCopy bool   // true for copy operation, false for insert
	Offset int    // source offset for copy
	Size   int    // size of data to copy
	Data   []byte // data to insert (for insert operation)
}

// Root command for the CLI
var RootCmd = &cobra.Command{
	Use:   "dbcli",
	Short: "CLI for managing the database",
	Long:  "A Command Line Interface (CLI) for managing collections and data in the custom database application.",
}

// Execute runs the root command
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error executing CLI: %v\n", err)
		os.Exit(1)
	}
}

func computeDelta(base, target []byte) []byte {
	// Delta format:
	// - First 4 bytes: size of source (base) content
	// - Next 4 bytes: size of target content
	// - Followed by a series of instructions (copy or insert)

	result := new(bytes.Buffer)

	// Write source size (base content size)
	binary.Write(result, binary.LittleEndian, uint32(len(base)))

	// Write target size
	binary.Write(result, binary.LittleEndian, uint32(len(target)))

	// Compute the operations needed to transform base into target
	// This is a simplified implementation of the delta algorithm
	// A real implementation would use a more efficient algorithm to find common substrings
	ops := computeDeltaOperations(base, target)

	// Write operations to the delta
	for _, op := range ops {
		if op.IsCopy {
			// Copy instruction starts with a byte where:
			// MSB is 1, followed by which of the next 4 bytes are present for offset and size
			// Then follow the offset and size bytes
			cmd := byte(0x80) // Set MSB to 1 for copy

			// Determine which bytes we need for offset
			offsetBytes := make([]byte, 0, 4)
			tempOffset := op.Offset
			for i := 0; i < 4; i++ {
				if tempOffset > 0 || i == 0 {
					offsetBytes = append(offsetBytes, byte(tempOffset&0xFF))
					tempOffset >>= 8
					cmd |= (1 << i) // Set bit to indicate this byte is present
				}
			}

			// Determine which bytes we need for size
			sizeBytes := make([]byte, 0, 3)
			tempSize := op.Size
			for i := 0; i < 3; i++ {
				if tempSize > 0 || i == 0 {
					sizeBytes = append(sizeBytes, byte(tempSize&0xFF))
					tempSize >>= 8
					cmd |= (1 << (i + 4)) // Offset by 4 to use the size bits
				}
			}

			// Write the command byte
			result.WriteByte(cmd)

			// Write offset bytes
			result.Write(offsetBytes)

			// Write size bytes
			result.Write(sizeBytes)
		} else {
			// Insert instruction:
			// Single byte with size (max 127 bytes per insert)
			// For larger inserts, split into multiple instructions

			data := op.Data
			for len(data) > 0 {
				chunkSize := len(data)
				if chunkSize > 127 {
					chunkSize = 127
				}

				// Write size byte (0-127)
				result.WriteByte(byte(chunkSize))

				// Write the data
				result.Write(data[:chunkSize])

				// Move to next chunk
				data = data[chunkSize:]
			}
		}
	}

	return result.Bytes()
}

func computeDeltaOperations(base, target []byte) []DeltaOperation {
	// This is a simplified approach. A real implementation would use
	// rolling hashes or suffix arrays for more efficient matching.

	var operations []DeltaOperation
	targetIndex := 0

	for targetIndex < len(target) {
		// Try to find a matching sequence in base
		bestMatchLen := 0
		bestMatchOffset := 0

		// Look for longest matching sequence
		// For simplicity, we use a brute force approach
		// A real implementation would use a more efficient algorithm
		if len(target)-targetIndex >= 4 { // Only look for matches of at least 4 bytes
			for baseIndex := 0; baseIndex < len(base); baseIndex++ {
				// Calculate max possible match length from this position
				maxMatchLen := 0
				for i := 0; i < min(len(base)-baseIndex, len(target)-targetIndex); i++ {
					if base[baseIndex+i] == target[targetIndex+i] {
						maxMatchLen++
					} else {
						break
					}
				}

				// If this match is better than our previous best, update
				if maxMatchLen > bestMatchLen {
					bestMatchLen = maxMatchLen
					bestMatchOffset = baseIndex
				}
			}
		}

		if bestMatchLen >= 4 { // Only use copy if match is at least 4 bytes
			// Add a copy operation
			operations = append(operations, DeltaOperation{
				IsCopy: true,
				Offset: bestMatchOffset,
				Size:   bestMatchLen,
			})
			targetIndex += bestMatchLen
		} else {
			// Find how many bytes don't match
			insertStart := targetIndex
			for targetIndex < len(target) {
				// Check if we can find a match of at least 4 bytes
				if targetIndex+4 <= len(target) {
					found := false
					for baseIndex := 0; baseIndex < len(base); baseIndex++ {
						if baseIndex+4 <= len(base) {
							matches := 0
							for i := 0; i < 4; i++ {
								if base[baseIndex+i] == target[targetIndex+i] {
									matches++
								} else {
									break
								}
							}
							if matches == 4 {
								found = true
								break
							}
						}
					}
					if found {
						break
					}
				}
				targetIndex++
				if targetIndex-insertStart >= 127 {
					// Maximum insert size in one operation, break here
					break
				}
			}

			// Add an insert operation
			operations = append(operations, DeltaOperation{
				IsCopy: false,
				Data:   target[insertStart:targetIndex],
			})
		}
	}

	return operations
}

func applyDelta(base, delta []byte) ([]byte, error) {
	if len(delta) < 8 {
		return nil, fmt.Errorf("delta too short")
	}

	// Read source size from first 4 bytes
	srcSize := binary.LittleEndian.Uint32(delta[0:4])
	if int(srcSize) != len(base) {
		return nil, fmt.Errorf("base size mismatch: expected %d, got %d", srcSize, len(base))
	}

	// Read target size from next 4 bytes
	targetSize := binary.LittleEndian.Uint32(delta[4:8])

	// Allocate result buffer
	result := make([]byte, 0, targetSize)

	// Process delta instructions
	i := 8 // Start after the header
	for i < len(delta) {
		cmd := delta[i]
		i++

		if cmd == 0 {
			// Reserved for future use
			return nil, fmt.Errorf("unexpected delta command 0")
		} else if cmd&0x80 != 0 {
			// Copy operation (MSB is set)
			offset := 0
			size := 0

			// Read offset (if corresponding bit is set)
			for j := 0; j < 4; j++ {
				if cmd&(1<<j) != 0 {
					offset |= int(delta[i]) << (j * 8)
					i++
				}
			}

			// Read size (if corresponding bit is set)
			for j := 0; j < 3; j++ {
				if cmd&(1<<(j+4)) != 0 {
					size |= int(delta[i]) << (j * 8)
					i++
				}
			}

			// If size is 0, use a special case of 0x10000
			if size == 0 {
				size = 0x10000
			}

			// Validate offset and size
			if offset+size > len(base) {
				return nil, fmt.Errorf("invalid copy operation: offset=%d, size=%d, base_len=%d",
					offset, size, len(base))
			}

			// Copy data from base
			result = append(result, base[offset:offset+size]...)
		} else {
			// Insert operation (copy from delta)
			size := int(cmd)
			if i+size > len(delta) {
				return nil, fmt.Errorf("invalid insert operation: size=%d, remaining=%d",
					size, len(delta)-i)
			}

			// Copy data from delta
			result = append(result, delta[i:i+size]...)
			i += size
		}
	}

	// Verify we produced the expected target size
	if len(result) != int(targetSize) {
		return nil, fmt.Errorf("result size mismatch: expected %d, got %d", targetSize, len(result))
	}

	return result, nil
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func writeDeltaObject(baseObjID string, delta []byte) (string, error) {
	// Format: "delta <base-sha> <size>\0<delta-data>"
	header := fmt.Sprintf("delta %s %d\u0000", baseObjID, len(delta))
	store := append([]byte(header), delta...)

	// Compute SHA of the combined content
	hash := sha1.Sum(store)
	sha := fmt.Sprintf("%x", hash)

	// Build path to store the delta object
	dir := sha[:2]
	name := sha[2:]
	objPath := filepath.Join(".nutella", "objects", dir, name)

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Join(".nutella", "objects", dir), 0755); err != nil {
		return "", fmt.Errorf("error creating object directory: %w", err)
	}

	// Compress and write the object
	var buf bytes.Buffer
	w := zlib.NewWriter(&buf)
	_, _ = w.Write(store)
	w.Close()

	if err := os.WriteFile(objPath, buf.Bytes(), 0644); err != nil {
		return "", fmt.Errorf("error writing delta object: %w", err)
	}

	return sha, nil
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

var packObjectsCmd = &cobra.Command{
	Use:   "pack <dbID>",
	Short: "Pack loose objects into a packfile",
	Long:  "This command packs loose objects in the repository into a packfile to save space",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		dbID := args[0]
		basePath, _ := filepath.Abs(filepath.Join("files", dbID))

		// Change to the database directory
		if err := os.Chdir(basePath); err != nil {
			fmt.Fprintf(os.Stderr, "Error changing directory: %v\n", err)
			os.Exit(1)
		}

		// Create a packfile
		packID := time.Now().Format("20060102-150405")
		packName := fmt.Sprintf("pack-%s", packID)
		packPath := filepath.Join(".nutella", "objects", "pack")

		// Ensure pack directory exists
		if err := os.MkdirAll(packPath, 0755); err != nil {
			fmt.Fprintf(os.Stderr, "Error creating pack directory: %v\n", err)
			os.Exit(1)
		}

		// Find all loose objects
		objectsDir := filepath.Join(".nutella", "objects")
		var objects []string

		// Walk the objects directory to find loose objects
		err := filepath.Walk(objectsDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			// Skip directories and pack files
			if info.IsDir() || strings.Contains(path, "pack") {
				return nil
			}

			// Get the object ID from the path
			dir := filepath.Base(filepath.Dir(path))
			file := filepath.Base(path)
			if len(dir) == 2 && len(file) == 38 {
				objects = append(objects, dir+file)
			}

			return nil
		})

		if err != nil {
			fmt.Fprintf(os.Stderr, "Error scanning objects: %v\n", err)
			os.Exit(1)
		}

		if len(objects) == 0 {
			fmt.Println("No loose objects found to pack")
			return
		}

		fmt.Printf("Found %d objects to pack\n", len(objects))

		// Sort objects by type and then by path to improve delta compression
		// In a real implementation, you'd sort them in a way that maximizes delta compression

		// Create a packfile
		packFile, err := os.Create(filepath.Join(packPath, packName+".pack"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating packfile: %v\n", err)
			os.Exit(1)
		}
		defer packFile.Close()

		// Write pack header: "PACK" signature, version (2), and number of objects
		packFile.Write([]byte("PACK"))
		binary.Write(packFile, binary.BigEndian, uint32(2)) // Version
		binary.Write(packFile, binary.BigEndian, uint32(len(objects)))

		// Create index file
		indexFile, err := os.Create(filepath.Join(packPath, packName+".idx"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating index file: %v\n", err)
			os.Exit(1)
		}
		defer indexFile.Close()

		// Write index header: the format depends on your implementation
		// For simplicity, we'll use a simple format: list of (sha, offset) pairs

		// Process objects
		for i, objID := range objects {
			// Read the object
			objData := readObject(objID)

			// Record the offset in the packfile
			offset, _ := packFile.Seek(0, io.SeekCurrent)

			// Write the object to the packfile
			// In a real implementation, you'd use delta compression between objects
			// For simplicity, we'll just write them as-is
			packFile.Write(objData)

			// Write the index entry
			binary.Write(indexFile, binary.BigEndian, objID)
			binary.Write(indexFile, binary.BigEndian, uint64(offset))

			if i%100 == 0 {
				fmt.Printf("Packed %d/%d objects\n", i, len(objects))
			}
		}

		fmt.Printf("Successfully packed %d objects into %s\n", len(objects), packName)

		// In a real implementation, you'd add an option to remove the loose objects
		// after successful packing
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

// Command to initialize a new nutella-like repository inside a database folder.
var handleInitCmd = &cobra.Command{
	Use:   "init [dbID]",
	Short: "Initialize a new nutella directory",
	Long:  "This command initializes a new nutella directory in the specified database folder.",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		database.HandleInit(args[0])
	},
}

// var handleStartServer = &cobra.Command{
// 	Use:   "startserver",
// 	Short: "Start the NutellaDB server",
// 	// koi API docs likhdo
// 	Long: "Starts the NutellaDB server as an API [see /docs for API documentation]",
// 	Run:  server.Server,
// }

// commitMessage will hold the commit message from the "-m" flag.
var commitMessage string

// Command to commit all changes and store the snapshot.
var handleCommitAllCmd = &cobra.Command{
	Use:   "commit-all <dbID>",
	Short: "Recursively hash files, create a tree and commit object for the given db",
	Long: `This command does the following:
  1. Uses the provided dbID to locate the repository at "./files/<dbID>".
  2. Loads ignore patterns from .nutignore.
  3. Recursively hashes all files in the repository (ignoring .nutella and matching ignore patterns).
  4. Writes a tree object for the entire directory structure.
  5. Creates and stores a commit object with the provided commit message.
  6. Stores the resulting commit hash, commit message, and a timestamp in snapshots.json with a unique UUID key.`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		dbID := args[0]
		basePath, _ := filepath.Abs(filepath.Join("files", dbID))

		// Verify that the repository exists.
		if _, err := os.Stat(filepath.Join(basePath, ".nutella")); os.IsNotExist(err) {
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

		// Load ignore patterns from .nutignore.
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

		sha, err := createAndStoreCommit(treeSha, commitMessage)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error storing commit: %s\n", err)
			os.Exit(1)
		}

		// Store the commit snapshot using the relative path.
		if err := storeSnapshot(sha, commitMessage); err != nil {
			fmt.Fprintf(os.Stderr, "Error storing snapshot: %s\n", err)
			os.Exit(1)
		}

		fmt.Println(sha)
	},
}

// createAndStoreCommit creates a commit object with the given tree SHA and commit message.
// It returns the computed commit hash.
func createAndStoreCommit(treeSha, message string) (string, error) {
	commitContent := fmt.Sprintf("tree %s\n\n%s\n", treeSha, message)
	header := fmt.Sprintf("commit %d\u0000", len(commitContent))
	store := append([]byte(header), []byte(commitContent)...)
	hash := sha1.Sum(store)
	sha := fmt.Sprintf("%x", hash)

	// Build path to store the commit object under .nutella/objects.
	dir := sha[:2]
	name := sha[2:]
	path := filepath.Join(".nutella", "objects", dir, name)

	if err := os.MkdirAll(filepath.Join(".nutella", "objects", dir), 0755); err != nil {
		return "", fmt.Errorf("Error creating commit directory: %w", err)
	}

	var buf bytes.Buffer
	w := zlib.NewWriter(&buf)
	_, _ = w.Write(store)
	w.Close()

	if err := os.WriteFile(path, buf.Bytes(), 0644); err != nil {
		return "", fmt.Errorf("Error writing commit: %w", err)
	}
	return sha, nil
}

// Snapshot represents a single commit snapshot.
type Snapshot struct {
	Commit    string `json:"commit"`
	Message   string `json:"message"`
	Timestamp string `json:"timestamp"`
}

// storeSnapshot updates the snapshots.json file (in the .nutella folder) by adding
// a new entry keyed by a UUID containing the commit hash, commit message, and the current timestamp.
func storeSnapshot(commitHash, commitMsg string) error {
	// Since we've already changed to the repository base, use a relative path.
	snapshotsPath := filepath.Join(".nutella", "snapshots.json")

	// Read existing snapshots file.
	var snapshots map[string]Snapshot
	data, err := os.ReadFile(snapshotsPath)
	if err != nil {
		// If file doesn't exist or cannot be read, start with an empty map.
		snapshots = make(map[string]Snapshot)
	} else {
		if err := json.Unmarshal(data, &snapshots); err != nil {
			// If unmarshalling fails, start fresh.
			snapshots = make(map[string]Snapshot)
		}
	}

	// Generate a new UUID as the key.
	id := uuid.New().String()
	// Use RFC3339 format for the timestamp.
	timestamp := time.Now().Format(time.RFC3339)

	// Append new snapshot with commit hash, message, and timestamp.
	snapshots[id] = Snapshot{
		Commit:    commitHash,
		Message:   commitMsg,
		Timestamp: timestamp,
	}

	// Marshal back to JSON.
	updatedData, err := json.MarshalIndent(snapshots, "", "  ")
	if err != nil {
		return fmt.Errorf("Error marshalling snapshots: %w", err)
	}

	// Write updated JSON back to snapshots.json.
	if err := os.WriteFile(snapshotsPath, updatedData, 0644); err != nil {
		return fmt.Errorf("Error writing snapshots file: %w", err)
	}
	return nil
}

// loadGitignore reads the .nutignore file and returns the list of ignore patterns.
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
		// Ignore the .nutella folder.
		if f.Name() == ".nutella" {
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
	objPath := filepath.Join(".nutella", "objects", dirName, fileName)

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

	// First, compute the regular blob object hash
	header := fmt.Sprintf("blob %d\u0000", len(content))
	store := append([]byte(header), content...)
	hash := sha1.Sum(store)
	sha := fmt.Sprintf("%x", hash)

	// Check if this object already exists
	dir := sha[:2]
	name := sha[2:]
	objPath := filepath.Join(".nutella", "objects", dir, name)
	if _, err := os.Stat(objPath); err == nil {
		// Object already exists, just return its SHA
		return sha, nil
	}

	// Find a similar object to use as a base for delta compression
	baseObjID, baseContent := findSimilarObject(content)

	if baseObjID != "" {
		// Compute delta
		delta := computeDelta(baseContent, content)

		// If delta is smaller than the original content (with some margin)
		if len(delta) < len(content)*9/10 {
			// Store as a delta object
			deltaSha, err := writeDeltaObject(baseObjID, delta)
			if err != nil {
				// Fall back to direct storage on error
				fmt.Fprintf(os.Stderr, "Warning: failed to write delta: %v\n", err)
			} else {
				return deltaSha, nil
			}
		}
	}

	// If we reach here, either no suitable base was found or delta wasn't efficient
	// Fall back to storing the full object
	if err := os.MkdirAll(filepath.Join(".nutella", "objects", dir), 0755); err != nil {
		return "", err
	}

	var buf bytes.Buffer
	w := zlib.NewWriter(&buf)
	_, _ = w.Write(store)
	w.Close()

	if err := os.WriteFile(objPath, buf.Bytes(), 0644); err != nil {
		return "", err
	}

	return sha, nil
}

func findSimilarObject(content []byte) (string, []byte) {
	// This is a simplified approach. A real implementation would index objects
	// by size or use other heuristics to find similar files quickly.
	objectsDir := filepath.Join(".nutella", "objects")

	// Look for blob objects only
	var bestMatch string
	var bestContent []byte
	var bestSimilarity float64

	// Walk the objects directory
	filepath.Walk(objectsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || len(filepath.Base(path)) != 38 {
			return nil // Skip directories and non-object files
		}

		// Read the object
		data, err := os.ReadFile(path)
		if err != nil {
			return nil
		}

		// Decompress the object
		r, err := zlib.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil
		}
		defer r.Close()

		objData, err := io.ReadAll(r)
		if err != nil {
			return nil
		}

		// Check if it's a blob object
		parts := bytes.SplitN(objData, []byte{0}, 2)
		if len(parts) != 2 || !bytes.HasPrefix(parts[0], []byte("blob ")) {
			return nil
		}

		objContent := parts[1]

		// Skip if sizes are too different
		if len(objContent) < len(content)/2 || len(objContent) > len(content)*2 {
			return nil
		}

		// Calculate similarity (simplified approach)
		// In a real implementation, you'd use a more sophisticated algorithm
		similarity := calculateSimilarity(objContent, content)

		if similarity > bestSimilarity && similarity > 0.6 { // 60% similarity threshold
			dir := filepath.Base(filepath.Dir(path))
			file := filepath.Base(path)
			bestMatch = dir + file
			bestContent = objContent
			bestSimilarity = similarity
		}

		return nil
	})

	return bestMatch, bestContent
}

func calculateSimilarity(a, b []byte) float64 {
	// This is a simplistic implementation. A real one would use better metrics.
	// For example, you might use Jaccard similarity on n-grams or other methods.

	// For this demo, we'll just sample bytes at regular intervals
	sampleSize := 100
	sampleCount := 0
	matchCount := 0

	// Skip if either is too small
	if len(a) < 10 || len(b) < 10 {
		return 0
	}

	// Sample at regular intervals
	stepA := max(1, len(a)/sampleSize)
	stepB := max(1, len(b)/sampleSize)

	for i := 0; i < min(len(a), sampleSize*stepA); i += stepA {
		for j := 0; j < min(len(b), sampleSize*stepB); j += stepB {
			sampleCount++
			if a[i] == b[j] {
				matchCount++
			}
		}
	}

	if sampleCount == 0 {
		return 0
	}

	return float64(matchCount) / float64(sampleCount)
}

// max returns the maximum of two integers
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// New Restore Command
var restoreCmd = &cobra.Command{
	Use:   "restore <dbname>",
	Short: "Restore a database to a previous commit snapshot",
	Long: `This command will:
  1. Change directory to the given database (./files/<dbname>).
  2. Load snapshots stored in .nutella/snapshots.json.
  3. Display the commit hash, commit message, and timestamp (sorted by time).
  4. Prompt for a commit hash to restore.
  5. Restore the working directory to that commit state.`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		dbName := args[0]
		basePath := filepath.Join(".", "files", dbName)

		// Change working directory to the repository base.
		if err := os.Chdir(basePath); err != nil {
			fmt.Fprintf(os.Stderr, "Error changing directory to %s: %v\n", basePath, err)
			os.Exit(1)
		}

		// Load snapshots from .nutella/snapshots.json.
		snapshots, err := loadSnapshots()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error loading snapshots: %v\n", err)
			os.Exit(1)
		}

		if len(snapshots) == 0 {
			fmt.Fprintf(os.Stderr, "No snapshots found.\n")
			os.Exit(1)
		}

		// Convert map to slice for sorting.
		type snapEntry struct {
			Key      string
			Snapshot Snapshot
		}
		var snapshotList []snapEntry
		for key, snap := range snapshots {
			snapshotList = append(snapshotList, snapEntry{Key: key, Snapshot: snap})
		}

		// Sort snapshots by timestamp.
		// If timestamps cannot be parsed, fallback to a simple string comparison.
		sort.Slice(snapshotList, func(i, j int) bool {
			ti, err1 := time.Parse(time.RFC3339, snapshotList[i].Snapshot.Timestamp)
			tj, err2 := time.Parse(time.RFC3339, snapshotList[j].Snapshot.Timestamp)
			if err1 != nil || err2 != nil {
				return snapshotList[i].Snapshot.Timestamp < snapshotList[j].Snapshot.Timestamp
			}
			return ti.Before(tj)
		})

		// Display snapshots.
		fmt.Println("Available snapshots:")
		for _, s := range snapshotList {
			fmt.Printf("Commit: %s | Message: %s | Timestamp: %s\n",
				s.Snapshot.Commit, s.Snapshot.Message, s.Snapshot.Timestamp)
		}

		// Prompt the user to select a commit hash.
		fmt.Print("Enter commit hash to restore: ")
		var chosen string
		fmt.Scanln(&chosen)

		// Verify that the chosen commit exists.
		found := false
		for _, s := range snapshotList {
			if s.Snapshot.Commit == chosen {
				found = true
				break
			}
		}
		if !found {
			fmt.Fprintf(os.Stderr, "Commit hash %s not found in snapshots.\n", chosen)
			os.Exit(1)
		}

		// Proceed to restore the chosen commit.
		restoreCommit(chosen)
	},
}

// New Restore Command
var restoreToCmd = &cobra.Command{
	Use:   "restore-to <dbname> <commit-hash>",
	Short: "Restore a database to a previous commit snapshot",
	Long: `This command will:
  1. Change directory to the given database (./files/<dbname>).
  2. Load snapshots stored in .nutella/snapshots.json.
  3. Display the commit hash, commit message, and timestamp (sorted by time).
  4. Prompt for a commit hash to restore.
  5. Restore the working directory to that commit state.`,
	Args: cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		dbName := args[0]
		basePath := filepath.Join(".", "files", dbName)

		// Change working directory to the repository base.
		if err := os.Chdir(basePath); err != nil {
			fmt.Fprintf(os.Stderr, "Error changing directory to %s: %v\n", basePath, err)
			os.Exit(1)
		}

		restoreCommit(args[1])
	},
}

// restoreCommit reads the commit object, extracts the tree SHA, cleans the directory,
// and restores the tree from that commit.
func restoreCommit(commitSha string) {
	data := readObject(commitSha)

	// Find the first null byte to separate header from content
	nullIndex := bytes.IndexByte(data, 0)
	if nullIndex == -1 {
		fmt.Fprintf(os.Stderr, "Invalid commit object: missing null byte\n")
		os.Exit(1)
	}
	body := data[nullIndex+1:]
	lines := bytes.Split(body, []byte("\n"))
	if len(lines) < 1 || !bytes.HasPrefix(lines[0], []byte("tree ")) {
		fmt.Fprintf(os.Stderr, "Invalid commit object: no tree reference found\n")
		os.Exit(1)
	}
	treeSha := string(bytes.TrimPrefix(lines[0], []byte("tree ")))

	// Load ignore patterns
	ignores, _ := loadGitignore()

	// Clean the current directory, preserving .nutella and .nutignore
	cleanCurrentDirectory(ignores)

	// Restore the tree - this will now handle delta objects through the readObject function
	restoreTree(treeSha, ".", "", ignores)

	fmt.Printf("Restored to commit %s\n", commitSha)
}

// loadSnapshots reads snapshots from .nutella/snapshots.json.
func loadSnapshots() (map[string]Snapshot, error) {
	data, err := os.ReadFile(filepath.Join(".nutella", "snapshots.json"))
	if err != nil {
		return nil, err
	}
	var snapshots map[string]Snapshot
	if err := json.Unmarshal(data, &snapshots); err != nil {
		return nil, err
	}
	return snapshots, nil
}

// readObject reads a stored object from .nutella/objects given its SHA.
func readObject(sha string) []byte {
	dir, name := sha[:2], sha[2:]
	path := filepath.Join(".nutella", "objects", dir, name)
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading object file: %v\n", err)
		os.Exit(1)
	}

	r, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating zlib reader: %v\n", err)
		os.Exit(1)
	}
	defer r.Close()

	decompressedData, err := io.ReadAll(r)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error decompressing data: %v\n", err)
		os.Exit(1)
	}

	// Check if this is a delta object
	if bytes.HasPrefix(decompressedData, []byte("delta ")) {
		// Parse the header to get base object ID and delta size
		nullIdx := bytes.IndexByte(decompressedData, 0)
		if nullIdx == -1 {
			fmt.Fprintf(os.Stderr, "Invalid delta object: missing null byte\n")
			os.Exit(1)
		}

		header := string(decompressedData[:nullIdx])
		parts := strings.Fields(header)
		if len(parts) != 3 {
			fmt.Fprintf(os.Stderr, "Invalid delta header: %s\n", header)
			os.Exit(1)
		}

		baseObjID := parts[1]

		// Get the delta data
		deltaData := decompressedData[nullIdx+1:]

		// Get the base object
		baseObj := readObject(baseObjID)

		// Extract the content from the base object
		baseNullIdx := bytes.IndexByte(baseObj, 0)
		if baseNullIdx == -1 {
			fmt.Fprintf(os.Stderr, "Invalid base object: missing null byte\n")
			os.Exit(1)
		}
		baseContent := baseObj[baseNullIdx+1:]

		// Apply the delta
		resultContent, err := applyDelta(baseContent, deltaData)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error applying delta: %v\n", err)
			os.Exit(1)
		}

		// Reconstruct the object with proper header
		// We need to determine the original object type from the base object
		baseHeader := string(baseObj[:baseNullIdx])
		baseParts := strings.Fields(baseHeader)
		if len(baseParts) < 1 {
			fmt.Fprintf(os.Stderr, "Invalid base object header: %s\n", baseHeader)
			os.Exit(1)
		}

		objType := baseParts[0]
		objHeader := fmt.Sprintf("%s %d", objType, len(resultContent))

		return append([]byte(objHeader+"\u0000"), resultContent...)
	}

	// Not a delta object, return as-is
	return decompressedData
}

// cleanCurrentDirectory removes all files and directories in the current directory,
// except for .nutella and .nutignore.
func cleanCurrentDirectory(ignores []string) {
	entries, err := os.ReadDir(".")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading current directory: %v\n", err)
		os.Exit(1)
	}

	for _, entry := range entries {
		name := entry.Name()
		if name == ".nutella" || name == ".nutignore" {
			continue
		}
		if shouldIgnore(name, ignores) {
			continue
		}
		if err := os.RemoveAll(name); err != nil {
			fmt.Fprintf(os.Stderr, "Error removing %s: %v\n", name, err)
		}
	}
}

// restoreTree recreates the files and directories from a tree object.
func restoreTree(treeSha, restorePath, repoRel string, ignores []string) {
	data := readObject(treeSha)

	nullIndex := bytes.IndexByte(data, 0)
	body := data[nullIndex+1:]
	i := 0
	for i < len(body) {
		modeEnd := bytes.IndexByte(body[i:], ' ')
		mode := string(body[i : i+modeEnd])
		i += modeEnd + 1
		nameEnd := bytes.IndexByte(body[i:], 0)
		name := string(body[i : i+nameEnd])
		i += nameEnd + 1
		entrySha := fmt.Sprintf("%x", body[i:i+20])
		i += 20

		relEntry := name
		if repoRel != "" {
			relEntry = filepath.Join(repoRel, name)
		}
		if shouldIgnore(relEntry, ignores) {
			continue
		}

		fullPath := filepath.Join(restorePath, name)
		if mode == "100644" {
			blobData := readObject(entrySha)
			nullIdx := bytes.IndexByte(blobData, 0)
			fileContent := blobData[nullIdx+1:]
			_ = os.MkdirAll(restorePath, 0755)
			if err := os.WriteFile(fullPath, fileContent, 0644); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to write file %s: %v\n", fullPath, err)
			}
		} else if mode == "40000" || mode == "040000" {
			if err := os.MkdirAll(fullPath, 0755); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to create directory %s: %v\n", fullPath, err)
				continue
			}
			newRepoRel := relEntry
			restoreTree(entrySha, fullPath, newRepoRel, ignores)
		}
	}
}

func Init() {
	RootCmd.AddCommand(createDBCmd)
	RootCmd.AddCommand(createCollectionCmd)
	RootCmd.AddCommand(insertCmd)
	RootCmd.AddCommand(findKeyCmd)
	RootCmd.AddCommand(updateCmd)
	RootCmd.AddCommand(deleteCmd)
	RootCmd.AddCommand(handleInitCmd)
	RootCmd.AddCommand(handleCommitAllCmd)
	handleCommitAllCmd.Flags().StringVarP(&commitMessage, "message", "m", "", "Commit message")
	RootCmd.AddCommand(restoreCmd)
	RootCmd.AddCommand(restoreToCmd)
	RootCmd.AddCommand(packObjectsCmd)

}
