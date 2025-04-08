package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
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
		if err := db.loadManifest(); err != nil {
			return nil, fmt.Errorf("failed to load manifest: %v", err)
		}
	} else {
		// Otherwise, create a new manifest
		if err := db.saveManifest(); err != nil {
			return nil, fmt.Errorf("failed to create new manifest: %v", err)
		}
	}

	return db, nil
}

func LoadDatabase(dbPath string) (*Database, error) {
	manifestPath := filepath.Join(dbPath, "manifest.json")
	data, err := ioutil.ReadFile(manifestPath)
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
	if err := db.saveManifest(); err != nil {
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

func (db *Database) saveManifest() error {
	data, err := json.MarshalIndent(db.manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal manifest: %v", err)
	}
	return ioutil.WriteFile(db.manifestPath, data, 0644)
}

func (db *Database) loadManifest() error {
	data, err := ioutil.ReadFile(db.manifestPath)
	if err != nil {
		return fmt.Errorf("failed to read manifest file: %v", err)
	}
	var m DBManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return fmt.Errorf("failed to unmarshal manifest: %v", err)
	}
	db.manifest = m
	return nil
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
	if err := db.saveManifest(); err != nil {
		return err
	}
	return nil
}
