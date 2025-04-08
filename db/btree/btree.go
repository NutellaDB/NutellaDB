package btree

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

func NewBTree(order int, collectionName string, pageDir string) (*BTree, error) {
    if order < 3 {
        return nil, fmt.Errorf("B-tree order must be at least 3")
    }

    // Make sure the pages dir exists
    if err := os.MkdirAll(pageDir, 0755); err != nil {
        return nil, fmt.Errorf("failed to create pages directory: %v", err)
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
        DBID:      collectionName, // or store the DB/collection name here
        PageDir:   pageDir,
        metadata:  &sync.RWMutex{},
        nodeCache: make(map[int]*Node),
    }

    // Save root node and metadata
    if err := bt.saveNode(root); err != nil {
        return nil, fmt.Errorf("failed to save root node: %v", err)
    }
    if err := bt.saveMetadata(); err != nil {
        return nil, fmt.Errorf("failed to save metadata: %v", err)
    }

    return bt, nil
}

// LoadBTree loads an existing B-tree from a pages directory
func LoadBTree(collectionName, pageDir string) (*BTree, error) {
    metadataPath := filepath.Join(pageDir, "metadata.json")
    data, err := os.ReadFile(metadataPath)
    if err != nil {
        return nil, fmt.Errorf("failed to read metadata file: %v", err)
    }

    bt := &BTree{
        metadata:  &sync.RWMutex{},
        nodeCache: make(map[int]*Node),
    }
    if err := json.Unmarshal(data, bt); err != nil {
        return nil, fmt.Errorf("failed to parse metadata: %v", err)
    }

    // Just in case, store the same pageDir
    bt.PageDir = pageDir
    // Optionally store the collection name
    bt.DBID = collectionName

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

	err = os.WriteFile(metadataPath, data, 0644)
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
