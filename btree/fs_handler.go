package btree

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

func (bt *BTree) saveNode(node *Node) error {

	bt.nodeCache[node.ID] = node

	nodePath := filepath.Join(bt.PageDir, fmt.Sprintf("page_%d.json", node.ID))
	data, err := json.MarshalIndent(node, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal node: %v", err)
	}

	err = os.WriteFile(nodePath, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to write node file: %v", err)
	}

	return nil
}

func (bt *BTree) loadNode(id int) (*Node, error) {

	// if node, ok := bt.nodeCache[id]; ok {
	// 	return node, nil
	// }

	nodePath := filepath.Join(bt.PageDir, fmt.Sprintf("page_%d.json", id))
	data, err := os.ReadFile(nodePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read node file: %v", err)
	}

	node := &Node{}
	err = json.Unmarshal(data, node)
	if err != nil {
		return nil, fmt.Errorf("failed to parse node: %v", err)
	}

	bt.nodeCache[id] = node

	return node, nil
}

func (bt *BTree) deleteNode(id int) error {

	delete(bt.nodeCache, id)

	nodePath := filepath.Join(bt.PageDir, fmt.Sprintf("page_%d.json", id))
	err := os.Remove(nodePath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete node file: %v", err)
	}

	return nil
}
