package main

import (
	"bytes"
	"compress/zlib"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

/*
 * main.go
 *
 * This is a simplified implementation of core nutella commands in Go.
 * In addition to existing commands, this version supports a new command:
 *   - commit-all: Recursively hashes all files (skipping files matching .gitignore)
 *                 then creates a tree and commit object in one go.
 *
 * Usage:
 *   mygit init
 *   mygit cat-file -p <sha>
 *   mygit hash-object [-w] <filename>
 *   mygit ls-tree [--name-only] <tree_sha>
 *   mygit write-tree
 *   mygit commit-tree <tree_sha> -m "<message>"
 *
 * New command:
 *   mygit commit-all -m "<message>"
 *     - This command will:
 *         1. Load ignore patterns from .gitignore (if it exists)
 *         2. Recursively hash all files (ignoring .nutella and matching ignore patterns)
 *         3. Write the tree object for the entire directory structure
 *         4. Create a commit object using the given commit message.
 */

func main() {
	fmt.Fprintf(os.Stderr, "Logs from your program will appear here!\n")

	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: mygit <command> [<args>...]\n")
		os.Exit(1)
	}

	switch command := os.Args[1]; command {
	case "init":
		handleInit()
	case "cat-file":
		handleCatFile()

	case "hash-object":
		handleHashObject()

	case "ls-tree":
		handleLsTree()

	case "write-tree":
		handleWriteTree()

	case "commit-tree":
		handleCommitTree()

	// New unified command: commit-all
	case "commit-all":
		handleCommitAll()

	case "restore":
		handleRestore()

	default:
		fmt.Fprintf(os.Stderr, "Unknown command %s\n", command)
		os.Exit(1)
	}
}

func handleCommitTree() {
	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "usage: mygit commit-tree <tree_sha> -m \"<message>\"\n")
		os.Exit(1)
	}
	treeSha := os.Args[2]
	message := os.Args[4]

	createAndStoreCommit(treeSha, message)
}

func handleCommitAll() {
	// Usage: mygit commit-all -m "<message>"
	if len(os.Args) < 4 || os.Args[2] != "-m" {
		fmt.Fprintf(os.Stderr, "usage: mygit commit-all -m \"<message>\"\n")
		os.Exit(1)
	}
	message := os.Args[3]
	// Load ignore patterns from .gitignore
	ignores, err := loadGitignore()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading .gitignore: %s\n", err)
		os.Exit(1)
	}
	// Create tree recursively from the current directory (".")
	treeSha, err := writeTreeRecursive(".", ".", ignores)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error writing tree: %s\n", err)
		os.Exit(1)
	}
	// Create and store commit object referencing the tree.
	createAndStoreCommit(treeSha, message)
}

func handleInit() {
	for _, dir := range []string{".nutella", ".nutella/objects", ".nutella/refs"} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			fmt.Fprintf(os.Stderr, "Error creating directory: %s\n", err)
		}
	}
	headFileContents := []byte("ref: refs/heads/main\n")
	if err := os.WriteFile(".nutella/HEAD", headFileContents, 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing file: %s\n", err)
	}
	fmt.Println("Initialized nutella directory")

}

// createAndStoreCommit creates a commit object with given tree SHA and commit message.
func createAndStoreCommit(treeSha, message string) {
	commitContent := fmt.Sprintf("tree %s\n\n%s\n", treeSha, message)
	header := fmt.Sprintf("commit %d\u0000", len(commitContent))
	store := append([]byte(header), []byte(commitContent)...)
	hash := sha1.Sum(store)
	sha := fmt.Sprintf("%x", hash)

	// Store commit in .nutella/objects
	dir := sha[:2]
	name := sha[2:]
	path := fmt.Sprintf(".nutella/objects/%s/%s", dir, name)

	if err := os.MkdirAll(fmt.Sprintf(".nutella/objects/%s", dir), 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating commit directory: %s\n", err)
		os.Exit(1)
	}

	var buf bytes.Buffer
	w := zlib.NewWriter(&buf)
	_, _ = w.Write(store)
	w.Close()

	if err := os.WriteFile(path, buf.Bytes(), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing commit: %s\n", err)
		os.Exit(1)
	}

	fmt.Println(sha)
}

func handleHashObject() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "usage: mygit hash-object [flags] <filename> \n")
		os.Exit(1)
	}
	write := false
	fileInd := 2
	if os.Args[2] == "-w" {
		write = true
		fileInd = 3
	}
	filename := os.Args[fileInd]
	hashFile(filename, write)
}

// loadGitignore reads the .gitignore file (if any) and returns the list of ignore patterns.
// Empty lines and lines starting with '#' are ignored.
func loadGitignore() ([]string, error) {
	data, err := os.ReadFile(".nutignore")
	if os.IsNotExist(err) {
		return []string{}, nil
	}
	if err != nil {
		return nil, err
	}

	lines := bytes.Split(data, []byte("\n"))
	var patterns []string
	for _, line := range lines {
		s := strings.TrimSpace(string(line))
		if s == "" || strings.HasPrefix(s, "#") {
			continue
		}
		patterns = append(patterns, s)
	}
	return patterns, nil
}

// shouldIgnore checks if the given relative path matches any of the ignore patterns.
func shouldIgnore(relPath string, patterns []string) bool {
	for _, pattern := range patterns {
		// Use filepath.Match to check the pattern.
		if matched, _ := filepath.Match(pattern, relPath); matched {
			return true
		}
		// Also check if any subpath matches (e.g. ignoring a directory)
		if strings.Contains(relPath, pattern) {
			return true
		}
	}
	return false
}

// writeTreeRecursive creates a tree object for the directory (relative to repo root).
// 'root' is the repository root (e.g., ".") and 'dir' is the current directory relative to root.
func writeTreeRecursive(root, dir string, ignores []string) (string, error) {
	var entries []byte

	fullDir := filepath.Join(root, dir)
	files, err := os.ReadDir(fullDir)
	if err != nil {
		return "", err
	}

	for _, f := range files {
		// Ignore the .nutella folder.
		if f.Name() == ".nutella" {
			continue
		}
		// Compute the relative path from repo root.
		relPath := f.Name()
		if dir != "." {
			relPath = filepath.Join(dir, f.Name())
		}
		if shouldIgnore(relPath, ignores) {
			// Skip file/directory if it matches .gitignore.
			continue
		}

		var mode string
		var sha string

		fullPath := filepath.Join(fullDir, f.Name())
		if f.IsDir() {
			mode = "40000"
			// Recursively write subtrees.
			sha, err = writeTreeRecursive(root, relPath, ignores)
			if err != nil {
				return "", err
			}
		} else {
			mode = "100644"
			sha, err = hashAndWriteBlob(fullPath)
			if err != nil {
				return "", err
			}
		}

		// Create the tree entry: "<mode> <filename>\0<sha>".
		entry := fmt.Sprintf("%s %s", mode, f.Name())
		entryBytes := []byte(entry)
		entryBytes = append(entryBytes, 0)
		shaRaw, _ := hex.DecodeString(sha)
		entryBytes = append(entryBytes, shaRaw...)
		entries = append(entries, entryBytes...)
	}

	header := fmt.Sprintf("tree %d\u0000", len(entries))
	store := append([]byte(header), entries...)
	hash := sha1.Sum(store)
	shaStr := fmt.Sprintf("%x", hash)
	dirName := shaStr[:2]
	fileName := shaStr[2:]
	objPath := fmt.Sprintf(".nutella/objects/%s/%s", dirName, fileName)

	if err := os.MkdirAll(filepath.Dir(objPath), 0755); err != nil {
		return "", err
	}
	var buf bytes.Buffer
	w := zlib.NewWriter(&buf)
	_, _ = w.Write(store)
	w.Close()
	if err := os.WriteFile(objPath, buf.Bytes(), 0644); err != nil {
		return "", err
	}
	return shaStr, nil
}

func handleCatFile() {
	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "usage: mygit cat-file -p [<args>...]\n")
		os.Exit(1)
	}
	sha_addr := os.Args[3]
	dir := sha_addr[:2]
	name := sha_addr[2:]
	path := fmt.Sprintf(".nutella/objects/%s/%s", dir, name)
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading object file: %s\n", err)
		os.Exit(1)
	}
	b := bytes.NewReader(data)
	r, err := zlib.NewReader(b)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating zlib reader: %s\n", err)
		os.Exit(1)
	}
	defer r.Close()
	decompressedData, err := io.ReadAll(r)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error decompressing data: %s\n", err)
		os.Exit(1)
	}
	nullIndex := bytes.IndexByte(decompressedData, 0)
	if nullIndex == -1 {
		fmt.Fprintf(os.Stderr, "Invalid nutella object format: missing null byte\n")
		os.Exit(1)
	}
	content := decompressedData[nullIndex+1:]
	fmt.Print(string(content))
}

func hashFile(filename string, write bool) {
	content, err := os.ReadFile(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading file: %s", err)
		os.Exit(1)
	}
	header := fmt.Sprintf("blob %d\u0000", len(content))
	store := append([]byte(header), content...)
	hash := sha1.Sum(store)
	hashStr := fmt.Sprintf("%x", hash)
	if write {
		dir := fmt.Sprintf(".nutella/objects/%s", hashStr[:2])
		if err := os.MkdirAll(dir, 0755); err != nil {
			fmt.Fprintf(os.Stderr, "Error creating object directory: %s \n", err)
			os.Exit(1)
		}
		var buf bytes.Buffer
		zw := zlib.NewWriter(&buf)
		_, err := zw.Write(store)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error compressing object: %s", err)
			os.Exit(1)
		}
		zw.Close()
		objectPath := fmt.Sprintf("%s/%s", dir, hashStr[2:])
		if err := os.WriteFile(objectPath, buf.Bytes(), 0644); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing object file: %s", err)
			os.Exit(1)
		}
	}
	fmt.Println(hashStr)
}

func handleLsTree() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "usage: mygit ls-tree [--name-only] <tree_sha>\n")
		os.Exit(1)
	}
	nameOnly := false
	sha := ""
	if os.Args[2] == "--name-only" {
		nameOnly = true
		if len(os.Args) < 4 {
			fmt.Fprintf(os.Stderr, "missing tree sha\n")
			os.Exit(1)
		}
		sha = os.Args[3]
	} else {
		sha = os.Args[2]
	}
	dir := sha[:2]
	name := sha[2:]
	path := fmt.Sprintf(".nutella/objects/%s/%s", dir, name)
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading tree object: %s\n", err)
		os.Exit(1)
	}
	b := bytes.NewReader(data)
	r, err := zlib.NewReader(b)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating zlib reader: %s\n", err)
		os.Exit(1)
	}
	defer r.Close()
	decompressed, err := io.ReadAll(r)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error decompressing object: %s\n", err)
		os.Exit(1)
	}
	nullIndex := bytes.IndexByte(decompressed, 0)
	if nullIndex == -1 {
		fmt.Fprintf(os.Stderr, "Invalid nutella object format: missing null byte\n")
		os.Exit(1)
	}
	data = decompressed[nullIndex+1:]
	i := 0
	for i < len(data) {
		modeEnd := bytes.IndexByte(data[i:], ' ')
		mode := string(data[i : i+modeEnd])
		i += modeEnd + 1
		nameEnd := bytes.IndexByte(data[i:], 0)
		filename := string(data[i : i+nameEnd])
		i += nameEnd + 1
		shaBytes := data[i : i+20]
		i += 20
		entrySha := fmt.Sprintf("%x", shaBytes)
		if nameOnly {
			fmt.Println(filename)
		} else {
			objType := "blob"
			if mode == "40000" || mode == "040000" {
				objType = "tree"
			}
			fmt.Printf("%06s %s %s\t%s\n", mode, objType, entrySha, filename)
		}
	}
}

func handleWriteTree() {
	sha, err := writeTreeRecursive(".", ".", []string{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error writing tree: %s\n", err)
		os.Exit(1)
	}
	fmt.Println(sha)
}

func hashAndWriteBlob(filename string) (string, error) {
	content, err := os.ReadFile(filename)
	if err != nil {
		return "", err
	}
	header := fmt.Sprintf("blob %d\u0000", len(content))
	store := append([]byte(header), content...)
	hash := sha1.Sum(store)
	sha := fmt.Sprintf("%x", hash)
	dir := sha[:2]
	name := sha[2:]
	path := fmt.Sprintf(".nutella/objects/%s/%s", dir, name)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.MkdirAll(fmt.Sprintf(".nutella/objects/%s", dir), 0755); err != nil {
			return "", err
		}
		var buf bytes.Buffer
		w := zlib.NewWriter(&buf)
		_, _ = w.Write(store)
		w.Close()
		if err := os.WriteFile(path, buf.Bytes(), 0644); err != nil {
			return "", err
		}
	}
	return sha, nil
}

func handleRestore() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "usage: mygit restore <commit_sha>\n")
		os.Exit(1)
	}

	// Load ignore patterns from .gitignore
	ignores, err := loadGitignore()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading .gitignore: %s\n", err)
		os.Exit(1)
	}

	commitSha := os.Args[2]
	data := readObject(commitSha)

	// Strip header: find the first null byte.
	nullIndex := bytes.IndexByte(data, 0)
	body := data[nullIndex+1:]

	lines := bytes.Split(body, []byte("\n"))
	if len(lines) < 1 || !bytes.HasPrefix(lines[0], []byte("tree ")) {
		fmt.Fprintf(os.Stderr, "Invalid commit object: no tree reference found\n")
		os.Exit(1)
	}
	treeSha := string(bytes.TrimPrefix(lines[0], []byte("tree ")))

	// Clean working directory while preserving ignored files.
	cleanCurrentDirectory(ignores)

	// Start restoring from the commit tree.
	restoreTree(treeSha, ".", "", ignores)

	fmt.Printf("Restored to commit %s\n", commitSha)
}

func cleanCurrentDirectory(ignores []string) {
	entries, err := os.ReadDir(".")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading current directory: %s\n", err)
		os.Exit(1)
	}

	for _, entry := range entries {
		name := entry.Name()
		// Always preserve .nutella and .gitignore.
		if name == ".nutella" || name == ".gitignore" {
			continue
		}
		// If the entry name matches any ignore pattern, skip removal.
		if shouldIgnore(name, ignores) {
			continue
		}
		if err := os.RemoveAll(name); err != nil {
			fmt.Fprintf(os.Stderr, "Error removing %s: %s\n", name, err)
		}
	}
}

func readObject(sha string) []byte {
	dir, name := sha[:2], sha[2:]
	path := fmt.Sprintf(".nutella/objects/%s/%s", dir, name)
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading object file: %s\n", err)
		os.Exit(1)
	}
	r, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating zlib reader: %s\n", err)
		os.Exit(1)
	}
	defer r.Close()
	decompressedData, err := io.ReadAll(r)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error decompressing data: %s\n", err)
		os.Exit(1)
	}
	return decompressedData
}

func restoreTree(treeSha, restorePath, repoRel string, ignores []string) {
	data := readObject(treeSha)

	// Skip header; retrieve the tree entries.
	nullIndex := bytes.IndexByte(data, 0)
	body := data[nullIndex+1:]
	i := 0
	for i < len(body) {
		// Read the mode.
		modeEnd := bytes.IndexByte(body[i:], ' ')
		mode := string(body[i : i+modeEnd])
		i += modeEnd + 1

		// Read the filename.
		nameEnd := bytes.IndexByte(body[i:], 0)
		name := string(body[i : i+nameEnd])
		i += nameEnd + 1

		// Read the SHA (20 bytes).
		entrySha := fmt.Sprintf("%x", body[i:i+20])
		i += 20

		// Compute the repository-relative path of this entry.
		relEntry := name
		if repoRel != "" {
			relEntry = filepath.Join(repoRel, name)
		}

		// If the ignore rules match, skip restoring this file/directory.
		if shouldIgnore(relEntry, ignores) {
			continue
		}

		fullPath := filepath.Join(restorePath, name)

		if mode == "100644" {
			// This is a blob (file).
			blobData := readObject(entrySha)
			nullIndex := bytes.IndexByte(blobData, 0)
			fileContent := blobData[nullIndex+1:]
			_ = os.MkdirAll(restorePath, 0755)
			if err := os.WriteFile(fullPath, fileContent, 0644); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to write file %s: %s\n", fullPath, err)
			}
		} else if mode == "40000" || mode == "040000" {
			// This is a directory. Create it and restore its subtree.
			if err := os.MkdirAll(fullPath, 0755); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to create directory %s: %s\n", fullPath, err)
				continue
			}
			// Update the repository relative path when going deeper.
			newRepoRel := relEntry
			restoreTree(entrySha, fullPath, newRepoRel, ignores)
		}
	}
}
