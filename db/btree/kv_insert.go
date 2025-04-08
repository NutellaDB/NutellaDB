package btree

import "fmt"

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

// allocateNodeID allocates a new node ID
func (bt *BTree) allocateNodeID() int {
	bt.metadata.Lock()
	defer bt.metadata.Unlock()

	id := bt.NextID
	bt.NextID++
	return id
}
