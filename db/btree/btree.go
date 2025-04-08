package btree

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

// KeyValue represents a key-value pair stored in the B-tree
type KeyValue struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

// Node represents a node in the B-tree
type Node struct {
	ID       int        `json:"id"`
	IsLeaf   bool       `json:"is_leaf"`
	Keys     []KeyValue `json:"keys"`
	Children []int      `json:"children"`
}

// BTree represents a B-tree
type BTree struct {
	RootID    int    `json:"root_id"`
	Order     int    `json:"order"`
	NextID    int    `json:"next_id"`
	DBID      string `json:"db_id"`
	PageDir   string `json:"page_dir"`
	metadata  *sync.RWMutex
	nodeCache map[int]*Node
}

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

// allocateNodeID allocates a new node ID
func (bt *BTree) allocateNodeID() int {
	bt.metadata.Lock()
	defer bt.metadata.Unlock()

	id := bt.NextID
	bt.NextID++
	return id
}

// Find searches for a key in the B-tree and returns its value
func (bt *BTree) Find(key string) (interface{}, bool, error) {
	// Load root node
	root, err := bt.loadNode(bt.RootID)
	if err != nil {
		return nil, false, fmt.Errorf("failed to load root node: %v", err)
	}

	// Search for the key
	return bt.findInNode(root, key)
}

// findInNode searches for a key in a node and its children
func (bt *BTree) findInNode(node *Node, key string) (interface{}, bool, error) {
	// Find the index where the key should be
	i := 0
	for i < len(node.Keys) && key > node.Keys[i].Key {
		i++
	}

	// Check if we found the key
	if i < len(node.Keys) && key == node.Keys[i].Key {
		return node.Keys[i].Value, true, nil
	}

	// If this is a leaf node, the key doesn't exist
	if node.IsLeaf {
		return nil, false, nil
	}

	// Recursively search the appropriate child
	child, err := bt.loadNode(node.Children[i])
	if err != nil {
		return nil, false, fmt.Errorf("failed to load child node: %v", err)
	}

	return bt.findInNode(child, key)
}

// Insert inserts a key-value pair into the B-tree
func (bt *BTree) Insert(key string, value interface{}) error {
	// Load root node
	root, err := bt.loadNode(bt.RootID)
	if err != nil {
		return fmt.Errorf("failed to load root node: %v", err)
	}

	// Check if the root node is full
	if len(root.Keys) == 2*bt.Order-1 {
		// Create a new root
		newRoot := &Node{
			ID:       bt.allocateNodeID(),
			IsLeaf:   false,
			Keys:     []KeyValue{},
			Children: []int{root.ID},
		}

		// Split the old root
		bt.splitChild(newRoot, 0, root)

		// Update the root ID
		bt.metadata.Lock()
		bt.RootID = newRoot.ID
		bt.metadata.Unlock()

		// Save the new root and metadata
		err = bt.saveNode(newRoot)
		if err != nil {
			return fmt.Errorf("failed to save new root node: %v", err)
		}
		err = bt.saveMetadata()
		if err != nil {
			return fmt.Errorf("failed to save metadata: %v", err)
		}

		// Insert into the new root
		return bt.insertNonFull(newRoot, key, value)
	}

	// Insert into the root
	return bt.insertNonFull(root, key, value)
}

// splitChild splits a full child of a node
func (bt *BTree) splitChild(parent *Node, index int, child *Node) error {
	// Create a new node
	newChild := &Node{
		ID:       bt.allocateNodeID(),
		IsLeaf:   child.IsLeaf,
		Keys:     make([]KeyValue, bt.Order-1),
		Children: make([]int, 0),
	}

	// Copy the second half of the child's keys to the new node
	copy(newChild.Keys, child.Keys[bt.Order:])

	// Copy the second half of the child's children to the new node (if not a leaf)
	if !child.IsLeaf {
		newChild.Children = make([]int, bt.Order)
		copy(newChild.Children, child.Children[bt.Order:])
		child.Children = child.Children[:bt.Order]
	}

	// Move the middle key to the parent
	middleKey := child.Keys[bt.Order-1]

	// Update the child's keys
	child.Keys = child.Keys[:bt.Order-1]

	// Insert the new child into the parent
	parent.Children = append(parent.Children, 0)
	copy(parent.Children[index+2:], parent.Children[index+1:])
	parent.Children[index+1] = newChild.ID

	// Insert the middle key into the parent
	parent.Keys = append(parent.Keys, KeyValue{})
	copy(parent.Keys[index+1:], parent.Keys[index:])
	parent.Keys[index] = middleKey

	// Save the modified nodes
	err := bt.saveNode(parent)
	if err != nil {
		return fmt.Errorf("failed to save parent node: %v", err)
	}
	err = bt.saveNode(child)
	if err != nil {
		return fmt.Errorf("failed to save child node: %v", err)
	}
	err = bt.saveNode(newChild)
	if err != nil {
		return fmt.Errorf("failed to save new child node: %v", err)
	}

	return nil
}

// insertNonFull inserts a key-value pair into a non-full node
func (bt *BTree) insertNonFull(node *Node, key string, value interface{}) error {
	// Find the position to insert the key
	i := len(node.Keys) - 1
	for i >= 0 && key < node.Keys[i].Key {
		i--
	}
	i++

	// If we found the key, update its value
	if i > 0 && i <= len(node.Keys) && node.Keys[i-1].Key == key {
		node.Keys[i-1].Value = value
		return bt.saveNode(node)
	}

	// If this is a leaf node, insert the key
	if node.IsLeaf {
		// Insert the key at the correct position
		node.Keys = append(node.Keys, KeyValue{})
		copy(node.Keys[i+1:], node.Keys[i:])
		node.Keys[i] = KeyValue{Key: key, Value: value}

		// Save the node
		return bt.saveNode(node)
	}

	// Load the child node
	child, err := bt.loadNode(node.Children[i])
	if err != nil {
		return fmt.Errorf("failed to load child node: %v", err)
	}

	// If the child is full, split it
	if len(child.Keys) == 2*bt.Order-1 {
		err = bt.splitChild(node, i, child)
		if err != nil {
			return fmt.Errorf("failed to split child: %v", err)
		}

		// Determine which child to follow
		if key > node.Keys[i].Key {
			child, err = bt.loadNode(node.Children[i+1])
			if err != nil {
				return fmt.Errorf("failed to load child node: %v", err)
			}
		} else if key == node.Keys[i].Key {
			// If the key already exists, update its value
			node.Keys[i].Value = value
			return bt.saveNode(node)
		} else {
			// Reload the child as it may have been modified
			child, err = bt.loadNode(node.Children[i])
			if err != nil {
				return fmt.Errorf("failed to load child node: %v", err)
			}
		}
	}

	// Insert into the child
	return bt.insertNonFull(child, key, value)
}

// Update updates the value of a key in the B-tree
func (bt *BTree) Update(key string, value interface{}) (bool, error) {
	// Load root node
	root, err := bt.loadNode(bt.RootID)
	if err != nil {
		return false, fmt.Errorf("failed to load root node: %v", err)
	}

	// Update the key
	return bt.updateInNode(root, key, value)
}

// updateInNode updates the value of a key in a node and its children
func (bt *BTree) updateInNode(node *Node, key string, value interface{}) (bool, error) {
	// Find the index where the key should be
	i := 0
	for i < len(node.Keys) && key > node.Keys[i].Key {
		i++
	}

	// Check if we found the key
	if i < len(node.Keys) && key == node.Keys[i].Key {
		// Update the value
		node.Keys[i].Value = value
		return true, bt.saveNode(node)
	}

	// If this is a leaf node, the key doesn't exist
	if node.IsLeaf {
		return false, nil
	}

	// Recursively update the appropriate child
	child, err := bt.loadNode(node.Children[i])
	if err != nil {
		return false, fmt.Errorf("failed to load child node: %v", err)
	}

	return bt.updateInNode(child, key, value)
}

// Delete deletes a key from the B-tree
func (bt *BTree) Delete(key string) (bool, error) {
	// Load root node
	root, err := bt.loadNode(bt.RootID)
	if err != nil {
		return false, fmt.Errorf("failed to load root node: %v", err)
	}

	// Delete the key
	deleted, err := bt.deleteFromNode(root, key)
	if err != nil {
		return false, err
	}

	// If the root is empty and has a child, make the child the new root
	if len(root.Keys) == 0 && !root.IsLeaf {
		bt.metadata.Lock()
		oldRootID := bt.RootID
		bt.RootID = root.Children[0]
		bt.metadata.Unlock()

		// Delete the old root
		err = bt.deleteNode(oldRootID)
		if err != nil {
			return deleted, fmt.Errorf("failed to delete old root: %v", err)
		}

		// Save metadata
		err = bt.saveMetadata()
		if err != nil {
			return deleted, fmt.Errorf("failed to save metadata: %v", err)
		}
	}

	return deleted, nil
}

// deleteFromNode deletes a key from a node and its children
func (bt *BTree) deleteFromNode(node *Node, key string) (bool, error) {
	// Find the index where the key should be
	i := 0
	for i < len(node.Keys) && key > node.Keys[i].Key {
		i++
	}

	// Case 1: The key is in this node
	if i < len(node.Keys) && key == node.Keys[i].Key {
		// Case 1a: If this is a leaf node, simply remove the key
		if node.IsLeaf {
			// Remove the key
			node.Keys = append(node.Keys[:i], node.Keys[i+1:]...)
			return true, bt.saveNode(node)
		}

		// Case 1b: If this is an internal node, find the predecessor or successor
		// Get the predecessor
		pred, err := bt.getPredecessor(node, i)
		if err != nil {
			return false, fmt.Errorf("failed to get predecessor: %v", err)
		}

		// Replace the key with the predecessor
		node.Keys[i] = pred

		// Delete the predecessor from the child
		childNode, err := bt.loadNode(node.Children[i])
		if err != nil {
			return false, fmt.Errorf("failed to load child node: %v", err)
		}

		// We need to ensure the child has at least t keys before deleting
		if len(childNode.Keys) < bt.Order {
			err = bt.ensureMinKeys(node, i)
			if err != nil {
				return false, fmt.Errorf("failed to ensure minimum keys: %v", err)
			}

			// Reload the child as it may have been modified
			childNode, err = bt.loadNode(node.Children[i])
			if err != nil {
				return false, fmt.Errorf("failed to load child node: %v", err)
			}
		}

		// Delete the predecessor from the child
		return bt.deleteFromNode(childNode, pred.Key)
	}

	// Case 2: The key is not in this node
	// If this is a leaf node, the key doesn't exist
	if node.IsLeaf {
		return false, nil
	}

	// Check if the child has at least t keys
	childNode, err := bt.loadNode(node.Children[i])
	if err != nil {
		return false, fmt.Errorf("failed to load child node: %v", err)
	}

	// Ensure the child has at least t keys
	if len(childNode.Keys) < bt.Order {
		err = bt.ensureMinKeys(node, i)
		if err != nil {
			return false, fmt.Errorf("failed to ensure minimum keys: %v", err)
		}

		// Reload the child as it may have been modified
		childNode, err = bt.loadNode(node.Children[i])
		if err != nil {
			return false, fmt.Errorf("failed to load child node: %v", err)
		}
	}

	// Recursively delete from the child
	return bt.deleteFromNode(childNode, key)
}

// getPredecessor finds the predecessor of a key in a node
func (bt *BTree) getPredecessor(node *Node, index int) (KeyValue, error) {
	// Get the child to the left of the key
	childID := node.Children[index]
	child, err := bt.loadNode(childID)
	if err != nil {
		return KeyValue{}, fmt.Errorf("failed to load child node: %v", err)
	}

	// Find the rightmost key in the subtree
	for !child.IsLeaf {
		childID = child.Children[len(child.Children)-1]
		child, err = bt.loadNode(childID)
		if err != nil {
			return KeyValue{}, fmt.Errorf("failed to load child node: %v", err)
		}
	}

	// Return the rightmost key in the leaf
	return child.Keys[len(child.Keys)-1], nil
}

// ensureMinKeys ensures that a child has at least t keys
func (bt *BTree) ensureMinKeys(node *Node, index int) error {
	// Get the child
	child, err := bt.loadNode(node.Children[index])
	if err != nil {
		return fmt.Errorf("failed to load child node: %v", err)
	}

	// If the child has at least t keys, we're done
	if len(child.Keys) >= bt.Order {
		return nil
	}

	// Try to borrow from the left sibling
	if index > 0 {
		leftSibling, err := bt.loadNode(node.Children[index-1])
		if err != nil {
			return fmt.Errorf("failed to load left sibling: %v", err)
		}

		if len(leftSibling.Keys) >= bt.Order {
			// Borrow a key from the left sibling
			// Move a key from the parent to the child
			child.Keys = append([]KeyValue{node.Keys[index-1]}, child.Keys...)

			// Move a key from the left sibling to the parent
			node.Keys[index-1] = leftSibling.Keys[len(leftSibling.Keys)-1]
			leftSibling.Keys = leftSibling.Keys[:len(leftSibling.Keys)-1]

			// Move a child from the left sibling to the child (if not a leaf)
			if !child.IsLeaf {
				child.Children = append([]int{leftSibling.Children[len(leftSibling.Children)-1]}, child.Children...)
				leftSibling.Children = leftSibling.Children[:len(leftSibling.Children)-1]
			}

			// Save the modified nodes
			err = bt.saveNode(node)
			if err != nil {
				return fmt.Errorf("failed to save node: %v", err)
			}
			err = bt.saveNode(child)
			if err != nil {
				return fmt.Errorf("failed to save child: %v", err)
			}
			err = bt.saveNode(leftSibling)
			if err != nil {
				return fmt.Errorf("failed to save left sibling: %v", err)
			}

			return nil
		}
	}

	// Try to borrow from the right sibling
	if index < len(node.Children)-1 {
		rightSibling, err := bt.loadNode(node.Children[index+1])
		if err != nil {
			return fmt.Errorf("failed to load right sibling: %v", err)
		}

		if len(rightSibling.Keys) >= bt.Order {
			// Borrow a key from the right sibling
			// Move a key from the parent to the child
			child.Keys = append(child.Keys, node.Keys[index])

			// Move a key from the right sibling to the parent
			node.Keys[index] = rightSibling.Keys[0]
			rightSibling.Keys = rightSibling.Keys[1:]

			// Move a child from the right sibling to the child (if not a leaf)
			if !child.IsLeaf {
				child.Children = append(child.Children, rightSibling.Children[0])
				rightSibling.Children = rightSibling.Children[1:]
			}

			// Save the modified nodes
			err = bt.saveNode(node)
			if err != nil {
				return fmt.Errorf("failed to save node: %v", err)
			}
			err = bt.saveNode(child)
			if err != nil {
				return fmt.Errorf("failed to save child: %v", err)
			}
			err = bt.saveNode(rightSibling)
			if err != nil {
				return fmt.Errorf("failed to save right sibling: %v", err)
			}

			return nil
		}
	}

	// Merge with a sibling
	if index > 0 {
		// Merge with the left sibling
		leftSibling, err := bt.loadNode(node.Children[index-1])
		if err != nil {
			return fmt.Errorf("failed to load left sibling: %v", err)
		}

		return bt.mergeNodes(node, index-1, leftSibling, child)
	} else {
		// Merge with the right sibling
		rightSibling, err := bt.loadNode(node.Children[index+1])
		if err != nil {
			return fmt.Errorf("failed to load right sibling: %v", err)
		}

		return bt.mergeNodes(node, index, child, rightSibling)
	}
}

// mergeNodes merges two adjacent child nodes
func (bt *BTree) mergeNodes(parent *Node, index int, left *Node, right *Node) error {
	// Move the key from the parent to the left node
	left.Keys = append(left.Keys, parent.Keys[index])

	// Remove the key from the parent
	parent.Keys = append(parent.Keys[:index], parent.Keys[index+1:]...)

	// Move all keys from the right node to the left node
	left.Keys = append(left.Keys, right.Keys...)

	// Move all children from the right node to the left node (if not a leaf)
	if !left.IsLeaf {
		left.Children = append(left.Children, right.Children...)
	}

	// Remove the right child from the parent
	parent.Children = append(parent.Children[:index+1], parent.Children[index+2:]...)

	// Save the modified nodes
	err := bt.saveNode(parent)
	if err != nil {
		return fmt.Errorf("failed to save parent: %v", err)
	}
	err = bt.saveNode(left)
	if err != nil {
		return fmt.Errorf("failed to save left node: %v", err)
	}

	// Delete the right node
	err = bt.deleteNode(right.ID)
	if err != nil {
		return fmt.Errorf("failed to delete right node: %v", err)
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
