package database

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"db/btree" // your existing B-tree package
)

// Focus Niggers.
// DBManifest tracks the DB ID plus a map of collection names to their subdirectory
type DBManifest struct {
	DBID        string            `json:"db_id"`
	Collections map[string]string `json:"collections"`
	// For example: {"c_1": "c_1", "c_2": "c_2"}
}

// Database wraps the manifest plus loaded collection objects
type Database struct {
	manifestPath string
	manifest     DBManifest
	collections  map[string]*Collection
	lock         sync.RWMutex
}

func handleInitRepository(basePath string) {
	// Create the .nutella folder within the basePath
	gitDir := filepath.Join(basePath, ".nutella")
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

	// Write the HEAD file within the .nutella directory.
	headFileContents := []byte("ref: refs/heads/main\n")
	headFilePath := filepath.Join(gitDir, "HEAD")
	if err := os.WriteFile(headFilePath, headFileContents, 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing HEAD file: %s\n", err)
	}

	// Create snapshots.json file inside the .nutella directory
	snapshotsFilePath := filepath.Join(gitDir, "snapshots.json")
	// Initialize with an empty JSON object.
	initialJSON := []byte("{}")
	if err := os.WriteFile(snapshotsFilePath, initialJSON, 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing snapshots.json file: %s\n", err)
	}

	fmt.Printf("Initialized nutella directory at %s\n", gitDir)
}

func HandleInit(dbID string) {
	basePath := filepath.Join(".", "files", dbID)

	if err := os.MkdirAll(basePath, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating base directory: %s\n", err)
		return
	}
	// Initialize nutella repository in the provided basePath
	handleInitRepository(basePath)
}

func NewDatabase(dbPath string, dbID string) (*Database, error) {
	// Create the database directory if not exists
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create db directory: %v", err)
	}

	manifestPath := filepath.Join(dbPath, "manifest.json")

	// Initialize an empty Database struct
	db := &Database{
		manifestPath: manifestPath,
		manifest: DBManifest{
			DBID:        dbID,
			Collections: make(map[string]string),
		},
		collections: make(map[string]*Collection),
	}

	// If manifest.json already exists, load it
	if _, err := os.Stat(manifestPath); err == nil {
		if _, err := db.LoadManifest(); err != nil {
			return nil, fmt.Errorf("failed to load manifest: %v", err)
		}
	} else {
		// Otherwise, create a new manifest
		if err := db.SaveManifest(); err != nil {
			return nil, fmt.Errorf("failed to create new manifest: %v", err)
		}
	}

	HandleInit(dbID)

	return db, nil
}

func LoadDatabase(dbPath string) (*Database, error) {
	manifestPath := filepath.Join(dbPath, "manifest.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest file: %v", err)
	}

	var m DBManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("failed to parse manifest: %v", err)
	}

	db := &Database{
		manifestPath: manifestPath,
		manifest:     m,
		collections:  make(map[string]*Collection),
	}

	// We don't automatically load all collections; we can load them on-demand
	// or load them here if you prefer. For now, they're lazily loaded.
	// Maybe Me and Dev cna refactor this later on

	return db, nil
}

// CreateCollection creates a subdir for this collection's B-tree
func (db *Database) CreateCollection(name string, order int) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	// Check if it already exists
	if _, exists := db.manifest.Collections[name]; exists {
		return fmt.Errorf("collection %q already exists", name)
	}

	// Make a subdirectory for the collection
	subDir := filepath.Join(filepath.Dir(db.manifestPath), name)
	if err := os.MkdirAll(subDir, 0755); err != nil {
		return fmt.Errorf("failed to create collection directory: %v", err)
	}

	// Create a new B-tree for this collection
	btreePath := filepath.Join(subDir, "pages")
	collBT, err := btree.NewBTree(order, name, btreePath)
	if err != nil {
		return fmt.Errorf("failed to create btree for collection %q: %v", name, err)
	}
	// We can close it immediately since no data has been inserted yet
	// or keep it open in a Collection struct
	coll := &Collection{
		name:    name,
		order:   order,
		btree:   collBT,
		baseDir: subDir,
	}
	db.collections[name] = coll

	// Update manifest
	db.manifest.Collections[name] = name
	if err := db.SaveManifest(); err != nil {
		return fmt.Errorf("failed to save manifest after creating collection: %v", err)
	}

	return nil
}

// GetCollection loads (if not already loaded) or returns a handle to the named collection
func (db *Database) GetCollection(name string) (*Collection, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	// If it's already in memory, return it
	if coll, ok := db.collections[name]; ok {
		return coll, nil
	}

	// Otherwise, see if it's in the manifest
	subDir, exists := db.manifest.Collections[name]
	if !exists {
		return nil, fmt.Errorf("collection %q not found in manifest", name)
	}

	// Load it
	pathToPages := filepath.Join(filepath.Dir(db.manifestPath), subDir, "pages")
	collBT, err := btree.LoadBTree(name, pathToPages)
	if err != nil {
		return nil, fmt.Errorf("failed to load btree for collection %q: %v", name, err)
	}

	coll := &Collection{
		name:    name,
		order:   collBT.Order,
		btree:   collBT,
		baseDir: filepath.Join(filepath.Dir(db.manifestPath), subDir),
	}
	db.collections[name] = coll

	return coll, nil
}

func (db *Database) GetAllCollections() ([]string, error) {
	db.LoadManifest()
	manifest, _ := db.LoadManifest()
	collections := manifest.Collections

	collections_array := make([]string, len(collections))
	i := 0

	for collection_key := range collections {
		collections_array[i] = collection_key
		i++
	}

	return collections_array, nil
}

func (db *Database) SaveManifest() error {
	data, err := json.MarshalIndent(db.manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal manifest: %v", err)
	}
	return os.WriteFile(db.manifestPath, data, 0644)
}

func (db *Database) LoadManifest() (DBManifest, error) {
	data, err := os.ReadFile(db.manifestPath)
	if err != nil {
		return DBManifest{}, fmt.Errorf("failed to read manifest file: %v", err)
	}
	var m DBManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return DBManifest{}, fmt.Errorf("failed to unmarshal manifest: %v", err)
	}
	db.manifest = m
	return m, nil
}

// Close closes all loaded collections
func (db *Database) Close() error {
	db.lock.Lock()
	defer db.lock.Unlock()

	for _, coll := range db.collections {
		if err := coll.btree.Close(); err != nil {
			return fmt.Errorf("failed closing collection %q: %v", coll.name, err)
		}
	}
	// Optionally save the manifest again
	if err := db.SaveManifest(); err != nil {
		return err
	}
	return nil
}

func ListDatabases(root string) ([]string, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, err
	}

	var dbIDs []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}

		manifest := filepath.Join(root, e.Name(), "manifest.json")
		if _, err := os.Stat(manifest); err == nil {
			dbIDs = append(dbIDs, e.Name())
		}
	}
	return dbIDs, nil
}
