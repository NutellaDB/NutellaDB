package btree

import "fmt"

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
