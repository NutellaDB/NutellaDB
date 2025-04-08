package btree

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

// saveNode saves a node to disk
func (bt *BTree) saveNode(node *Node) error {
	// Add node to cache
	bt.nodeCache[node.ID] = node

	// Write node to disk
	nodePath := filepath.Join(bt.PageDir, fmt.Sprintf("page_%d.json", node.ID))
	data, err := json.MarshalIndent(node, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal node: %v", err)
	}

	err = ioutil.WriteFile(nodePath, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to write node file: %v", err)
	}

	return nil
}

// loadNode loads a node from disk
func (bt *BTree) loadNode(id int) (*Node, error) {
	// Check if the node is already in the cache
	if node, ok := bt.nodeCache[id]; ok {
		return node, nil
	}

	// Read node file
	nodePath := filepath.Join(bt.PageDir, fmt.Sprintf("page_%d.json", id))
	data, err := ioutil.ReadFile(nodePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read node file: %v", err)
	}

	// Parse node
	node := &Node{}
	err = json.Unmarshal(data, node)
	if err != nil {
		return nil, fmt.Errorf("failed to parse node: %v", err)
	}

	// Add node to cache
	bt.nodeCache[id] = node

	return node, nil
}

// deleteNode deletes a node from disk
func (bt *BTree) deleteNode(id int) error {
	// Remove node from cache
	delete(bt.nodeCache, id)

	// Delete node file
	nodePath := filepath.Join(bt.PageDir, fmt.Sprintf("page_%d.json", id))
	err := os.Remove(nodePath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete node file: %v", err)
	}

	return nil
}
