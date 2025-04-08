package btree

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

// NewBTree creates a new B-tree with the specified order and DB ID
func NewBTree(order int, dbID string) (*BTree, error) {
	if order < 3 {
		return nil, fmt.Errorf("B-tree order must be at least 3")
	}

	// Create directory structure
	pageDir := filepath.Join(".", "files", dbID, "pages")
	err := os.MkdirAll(pageDir, 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to create directory structure: %v", err)
	}

	// Create root node
	root := &Node{
		ID:       1,
		IsLeaf:   true,
		Keys:     []KeyValue{},
		Children: []int{},
	}

	// Create B-tree
	bt := &BTree{
		RootID:    root.ID,
		Order:     order,
		NextID:    2,
		DBID:      dbID,
		PageDir:   pageDir,
		metadata:  &sync.RWMutex{},
		nodeCache: make(map[int]*Node),
	}

	// Save root node and metadata
	err = bt.saveNode(root)
	if err != nil {
		return nil, fmt.Errorf("failed to save root node: %v", err)
	}
	err = bt.saveMetadata()
	if err != nil {
		return nil, fmt.Errorf("failed to save metadata: %v", err)
	}

	return bt, nil
}

// LoadBTree loads an existing B-tree from the specified DB ID
func LoadBTree(dbID string) (*BTree, error) {
	pageDir := filepath.Join(".", "files", dbID, "pages")
	metadataPath := filepath.Join(pageDir, "metadata.json")

	// Read metadata file
	data, err := ioutil.ReadFile(metadataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata file: %v", err)
	}

	// Parse metadata
	bt := &BTree{
		metadata:  &sync.RWMutex{},
		nodeCache: make(map[int]*Node),
	}
	err = json.Unmarshal(data, bt)
	if err != nil {
		return nil, fmt.Errorf("failed to parse metadata: %v", err)
	}

	return bt, nil
}

// saveMetadata saves the B-tree metadata to disk
func (bt *BTree) saveMetadata() error {
	bt.metadata.RLock()
	defer bt.metadata.RUnlock()

	metadataPath := filepath.Join(bt.PageDir, "metadata.json")
	data, err := json.MarshalIndent(bt, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %v", err)
	}

	err = ioutil.WriteFile(metadataPath, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to write metadata file: %v", err)
	}

	return nil
}

// Close closes the B-tree and frees resources
func (bt *BTree) Close() error {
	// Save metadata
	err := bt.saveMetadata()
	if err != nil {
		return fmt.Errorf("failed to save metadata: %v", err)
	}

	// Clear cache
	bt.nodeCache = make(map[int]*Node)

	return nil
}
