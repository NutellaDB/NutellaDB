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

	if err := os.MkdirAll(pageDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create pages directory: %v", err)
	}

	root := &Node{
		ID:       1,
		IsLeaf:   true,
		Keys:     []KeyValue{},
		Children: []int{},
	}

	bt := &BTree{
		RootID:    root.ID,
		Order:     order,
		NextID:    2,
		DBID:      collectionName,
		PageDir:   pageDir,
		metadata:  &sync.RWMutex{},
		nodeCache: make(map[int]*Node),
	}

	if err := bt.saveNode(root); err != nil {
		return nil, fmt.Errorf("failed to save root node: %v", err)
	}
	if err := bt.saveMetadata(); err != nil {
		return nil, fmt.Errorf("failed to save metadata: %v", err)
	}

	return bt, nil
}

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

	bt.PageDir = pageDir

	bt.DBID = collectionName

	return bt, nil
}

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

func (bt *BTree) Close() error {

	err := bt.saveMetadata()
	if err != nil {
		return fmt.Errorf("failed to save metadata: %v", err)
	}

	bt.nodeCache = make(map[int]*Node)

	return nil
}
