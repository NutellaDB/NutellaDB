package btree

import (
	"fmt"
	"os"
)

// Delete deletes a key from the B-tree
func (bt *BTree) Delete(key string) (bool, error) {
	// Load root node
	root, err := bt.loadNode(bt.RootID)
	if err != nil {
		// If root doesn't exist, the tree is empty
		if os.IsNotExist(err) {
			return false, nil
		}
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
		if err != nil && !os.IsNotExist(err) {
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

// loadNodeSafe attempts to load a node and returns nil if the node doesn't exist
func (bt *BTree) loadNodeSafe(nodeID int) (*Node, error) {
	node, err := bt.loadNode(nodeID)
	if err != nil {
		if os.IsNotExist(err) {
			// Log this issue but don't fail the operation
			fmt.Printf("Warning: Node %d doesn't exist, ignoring\n", nodeID)
			return nil, nil
		}
		return nil, err
	}
	return node, nil
}

// nodeExists checks if a node file exists
func (bt *BTree) nodeExists(nodeID int) bool {
	filename := bt.getNodeFilename(nodeID)
	_, err := os.Stat(filename)
	return !os.IsNotExist(err)
}

// getNodeFilename returns the filename for a node
func (bt *BTree) getNodeFilename(nodeID int) string {
	return fmt.Sprintf("files/%s/pages/page_%d.json", bt.DBID, nodeID)
}

// deleteFromNode deletes a key from a node and its children
func (bt *BTree) deleteFromNode(node *Node, key string) (bool, error) {
	// If node is nil (doesn't exist), the key doesn't exist
	if node == nil {
		return false, nil
	}

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
		// Check if the child exists before proceeding
		if i < len(node.Children) && !bt.nodeExists(node.Children[i]) {
			// Child doesn't exist, handle the inconsistency
			// Remove this key and its child references
			node.Keys = append(node.Keys[:i], node.Keys[i+1:]...)
			node.Children = append(node.Children[:i], node.Children[i+1:]...)
			return true, bt.saveNode(node)
		}

		// Get the predecessor
		pred, err := bt.getPredecessor(node, i)
		if err != nil {
			// If we can't get the predecessor, try removing the key directly
			node.Keys = append(node.Keys[:i], node.Keys[i+1:]...)
			if i < len(node.Children) {
				node.Children = append(node.Children[:i], node.Children[i+1:]...)
			}
			return true, bt.saveNode(node)
		}

		// Replace the key with the predecessor
		node.Keys[i] = pred

		// Delete the predecessor from the child
		childNode, err := bt.loadNodeSafe(node.Children[i])
		if err != nil {
			return false, fmt.Errorf("failed to load child node: %v", err)
		}

		// If child doesn't exist, handle the inconsistency
		if childNode == nil {
			// Remove this key and its child references
			node.Keys = append(node.Keys[:i], node.Keys[i+1:]...)
			node.Children = append(node.Children[:i], node.Children[i+1:]...)
			return true, bt.saveNode(node)
		}

		// We need to ensure the child has at least t keys before deleting
		if len(childNode.Keys) < bt.Order {
			err = bt.ensureMinKeys(node, i)
			if err != nil {
				// If ensuring min keys fails, try to continue anyway
				fmt.Printf("Warning: Failed to ensure minimum keys: %v\n", err)
			}

			// Reload the child as it may have been modified
			childNode, err = bt.loadNodeSafe(node.Children[i])
			if err != nil {
				return false, fmt.Errorf("failed to load child node: %v", err)
			}

			// If child no longer exists after ensuring min keys, save the node and return
			if childNode == nil {
				return true, bt.saveNode(node)
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

	// Check if the child index is valid
	if i >= len(node.Children) {
		return false, nil
	}

	// Check if the child exists
	if !bt.nodeExists(node.Children[i]) {
		// Child doesn't exist, remove it from the parent
		if i < len(node.Children) {
			node.Children = append(node.Children[:i], node.Children[i+1:]...)
		}
		// If this makes the node invalid, fix it
		if len(node.Children) <= i && i > 0 && len(node.Keys) >= i {
			node.Keys = node.Keys[:i-1]
		}
		return false, bt.saveNode(node)
	}

	// Load the child
	childNode, err := bt.loadNodeSafe(node.Children[i])
	if err != nil {
		return false, fmt.Errorf("failed to load child node: %v", err)
	}

	// If child doesn't exist, handle the inconsistency
	if childNode == nil {
		// Remove this child reference
		node.Children = append(node.Children[:i], node.Children[i+1:]...)
		if len(node.Keys) > i {
			node.Keys = append(node.Keys[:i], node.Keys[i+1:]...)
		}
		return false, bt.saveNode(node)
	}

	// Ensure the child has at least t keys
	if len(childNode.Keys) < bt.Order {
		err = bt.ensureMinKeys(node, i)
		if err != nil {
			// Log the error but try to continue
			fmt.Printf("Warning: Failed to ensure minimum keys: %v\n", err)
		}

		// Reload the child as it may have been modified
		if i < len(node.Children) {
			childNode, err = bt.loadNodeSafe(node.Children[i])
			if err != nil {
				return false, fmt.Errorf("failed to load child node: %v", err)
			}

			// If child no longer exists after ensuring min keys, save the node and return
			if childNode == nil {
				return false, bt.saveNode(node)
			}
		} else {
			// The index is now out of bounds, so the key doesn't exist
			return false, nil
		}
	}

	// Recursively delete from the child
	return bt.deleteFromNode(childNode, key)
}

// getPredecessor finds the predecessor of a key in a node
func (bt *BTree) getPredecessor(node *Node, index int) (KeyValue, error) {
	// Validate index
	if index < 0 || index >= len(node.Children) {
		return KeyValue{}, fmt.Errorf("invalid index for getPredecessor")
	}

	// Get the child to the left of the key
	childID := node.Children[index]

	// Check if child exists
	if !bt.nodeExists(childID) {
		return KeyValue{}, fmt.Errorf("child node doesn't exist")
	}

	child, err := bt.loadNode(childID)
	if err != nil {
		return KeyValue{}, fmt.Errorf("failed to load child node: %v", err)
	}

	// Find the rightmost key in the subtree
	for !child.IsLeaf {
		if len(child.Children) == 0 {
			// This should not happen in a valid B-tree, but handle it
			return KeyValue{}, fmt.Errorf("internal node has no children")
		}

		childID = child.Children[len(child.Children)-1]

		// Check if the next child exists
		if !bt.nodeExists(childID) {
			// If not, use the current node's last key
			if len(child.Keys) > 0 {
				return child.Keys[len(child.Keys)-1], nil
			}
			return KeyValue{}, fmt.Errorf("cannot find predecessor")
		}

		child, err = bt.loadNode(childID)
		if err != nil {
			return KeyValue{}, fmt.Errorf("failed to load child node: %v", err)
		}
	}

	// Check if the leaf has keys
	if len(child.Keys) == 0 {
		return KeyValue{}, fmt.Errorf("leaf node has no keys")
	}

	// Return the rightmost key in the leaf
	return child.Keys[len(child.Keys)-1], nil
}

// ensureMinKeys ensures that a child has at least t keys
func (bt *BTree) ensureMinKeys(node *Node, index int) error {
	// Validate index
	if index < 0 || index >= len(node.Children) {
		return fmt.Errorf("invalid index for ensureMinKeys")
	}

	// Check if child exists
	if !bt.nodeExists(node.Children[index]) {
		// Remove this child reference
		node.Children = append(node.Children[:index], node.Children[index+1:]...)
		if len(node.Keys) > index {
			node.Keys = append(node.Keys[:index], node.Keys[index+1:]...)
		}
		return bt.saveNode(node)
	}

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
		// Check if left sibling exists
		if !bt.nodeExists(node.Children[index-1]) {
			// Remove the reference to the non-existent sibling
			node.Children = append(node.Children[:index-1], node.Children[index:]...)
			if len(node.Keys) > index-1 {
				node.Keys = append(node.Keys[:index-1], node.Keys[index:]...)
			}
			return bt.saveNode(node)
		}

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
			if !child.IsLeaf && len(leftSibling.Children) > 0 {
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
		// Check if right sibling exists
		if !bt.nodeExists(node.Children[index+1]) {
			// Remove the reference to the non-existent sibling
			node.Children = append(node.Children[:index+1], node.Children[index+2:]...)
			if len(node.Keys) > index {
				node.Keys = append(node.Keys[:index], node.Keys[index+1:]...)
			}
			return bt.saveNode(node)
		}

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
			if !child.IsLeaf && len(rightSibling.Children) > 0 {
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
		// Check if left sibling exists
		if !bt.nodeExists(node.Children[index-1]) {
			// If left sibling doesn't exist, try right sibling or fail
			if index < len(node.Children)-1 && bt.nodeExists(node.Children[index+1]) {
				rightSibling, err := bt.loadNode(node.Children[index+1])
				if err != nil {
					return fmt.Errorf("failed to load right sibling: %v", err)
				}
				return bt.mergeNodes(node, index, child, rightSibling)
			}

			// Remove the reference to the non-existent child
			node.Children = append(node.Children[:index-1], node.Children[index:]...)
			if len(node.Keys) > index-1 {
				node.Keys = append(node.Keys[:index-1], node.Keys[index:]...)
			}
			return bt.saveNode(node)
		}

		// Merge with the left sibling
		leftSibling, err := bt.loadNode(node.Children[index-1])
		if err != nil {
			return fmt.Errorf("failed to load left sibling: %v", err)
		}

		return bt.mergeNodes(node, index-1, leftSibling, child)
	} else {
		// Check if right sibling exists
		if index >= len(node.Children)-1 || !bt.nodeExists(node.Children[index+1]) {
			// Remove the reference to the non-existent child
			node.Children = append(node.Children[:index], node.Children[index+1:]...)
			if len(node.Keys) > index {
				node.Keys = append(node.Keys[:index], node.Keys[index+1:]...)
			}
			return bt.saveNode(node)
		}

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
	// Make sure both nodes exist
	if left == nil || right == nil {
		return fmt.Errorf("cannot merge nil nodes")
	}

	// Validate index
	if index < 0 || index >= len(parent.Keys) {
		return fmt.Errorf("invalid index for mergeNodes")
	}

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
	if index+1 < len(parent.Children) {
		parent.Children = append(parent.Children[:index+1], parent.Children[index+2:]...)
	} else {
		parent.Children = parent.Children[:index+1]
	}

	// Save the modified nodes
	err := bt.saveNode(parent)
	if err != nil {
		return fmt.Errorf("failed to save parent: %v", err)
	}
	err = bt.saveNode(left)
	if err != nil {
		return fmt.Errorf("failed to save left node: %v", err)
	}

	// Delete the right node, but don't worry if it's already gone
	err = bt.deleteNode(right.ID)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete right node: %v", err)
	}

	return nil
}

// RepairTree attempts to repair any inconsistencies in the B-tree
func (bt *BTree) RepairTree() error {
	// Load root node
	root, err := bt.loadNode(bt.RootID)
	if err != nil {
		if os.IsNotExist(err) {
			// Root doesn't exist, create a new empty root
			root = &Node{
				ID:       bt.RootID,
				IsLeaf:   true,
				Keys:     []KeyValue{},
				Children: []int{},
			}
			return bt.saveNode(root)
		}
		return fmt.Errorf("failed to load root node: %v", err)
	}

	// Check for consistency recursively
	return bt.repairNode(root)
}

// repairNode recursively checks and repairs a node and its children
func (bt *BTree) repairNode(node *Node) error {
	if node == nil {
		return nil
	}

	// Check if all children exist
	if !node.IsLeaf {
		validChildren := []int{}
		validKeys := []KeyValue{}

		for i, childID := range node.Children {
			if bt.nodeExists(childID) {
				validChildren = append(validChildren, childID)

				// Keep corresponding keys
				if i > 0 && i-1 < len(node.Keys) {
					validKeys = append(validKeys, node.Keys[i-1])
				}

				// Recursively repair child
				child, err := bt.loadNode(childID)
				if err == nil {
					err = bt.repairNode(child)
					if err != nil {
						fmt.Printf("Warning: Failed to repair child %d: %v\n", childID, err)
					}
				}
			}
		}

		// Update node with valid children and keys
		node.Children = validChildren
		node.Keys = validKeys

		// If node is now empty, make it a leaf
		if len(node.Children) == 0 {
			node.IsLeaf = true
		}

		// Save the repaired node
		return bt.saveNode(node)
	}

	return bt.saveNode(node)
}
