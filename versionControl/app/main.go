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
)

/*
 * main.go
 *
 * This is a simplified implementation of core Git commands in Go.
 * Supported commands include:
 *   - init: Initializes a new Git repository.
 *   - cat-file: Displays the content of a Git object.
 *   - hash-object: Hashes a file and optionally stores it as a Git object.
 *   - ls-tree: Lists the contents of a tree object.
 *   - write-tree: Creates a tree object from the current working directory.
 *   - commit-tree: Creates a commit object referencing a tree.
 *   - restore: Restores files (implementation assumed in external handler).
 */

// Usage: your_program.sh <command> <arg1> <arg2> ...
func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Fprintf(os.Stderr, "Logs from your program will appear here!\n")

	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: mygit <command> [<args>...]\n")
		os.Exit(1)
	}

	switch command := os.Args[1]; command {
	case "init":
		// Uncomment this block to pass the first stage!
		//
		for _, dir := range []string{".git", ".git/objects", ".git/refs"} {
			if err := os.MkdirAll(dir, 0755); err != nil {
				fmt.Fprintf(os.Stderr, "Error creating directory: %s\n", err)
			}
		}

		headFileContents := []byte("ref: refs/heads/main\n")
		if err := os.WriteFile(".git/HEAD", headFileContents, 0644); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing file: %s\n", err)
		}

		fmt.Println("Initialized git directory")

	case "cat-file":
		handleCatFile()

	case "hash-object":

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

	case "ls-tree":
		handleLsTree()

	case "write-tree":
		handleWriteTree()

	case "commit-tree":
		if len(os.Args) < 4 {
			fmt.Fprintf(os.Stderr, "usage: mygit commit-tree <tree_sha> -m \"<message>\"\n")
			os.Exit(1)
		}

		treeSha := os.Args[2]
		message := os.Args[4]

		commitContent := fmt.Sprintf("tree %s\n\n%s\n", treeSha, message)
		header := fmt.Sprintf("commit %d\u0000", len(commitContent))
		store := append([]byte(header), []byte(commitContent)...)

		hash := sha1.Sum(store)
		sha := fmt.Sprintf("%x", hash)

		// Store in .git/objects
		dir := sha[:2]
		name := sha[2:]
		path := fmt.Sprintf(".git/objects/%s/%s", dir, name)

		if err := os.MkdirAll(fmt.Sprintf(".git/objects/%s", dir), 0755); err != nil {
			fmt.Fprintf(os.Stderr, "Error creating directory: %s\n", err)
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

	case "restore":
		handleRestore()

	default:
		fmt.Fprintf(os.Stderr, "Unknown command %s\n", command)
		os.Exit(1)
	}
}

func handleCatFile() {
	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "usage: mygit cat-file -p [<args>...]\n")
		os.Exit(1)
	}

	sha_addr := os.Args[3]
	dir := sha_addr[:2]
	name := sha_addr[2:]

	path := fmt.Sprintf(".git/objects/%s/%s", dir, name)

	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading object file: %s\n", err)
		os.Exit(1)
	}

	// zlip decompression

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

	// find the index where null occurs
	nullIndex := bytes.IndexByte(decompressedData, 0)
	if nullIndex == -1 {
		fmt.Fprintf(os.Stderr, "Invalid Git object format: missing null byte\n")
		os.Exit(1)
	}

	content := decompressedData[nullIndex+1:]
	fmt.Print(string(content))
}

func hashFile(filename string, write bool) {

	// Hashes the file content

	content, err := os.ReadFile(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading file: %s", err)
		os.Exit(1)
	}

	header := fmt.Sprintf("blob %d\u0000", len(content))
	store := append([]byte(header), content...)

	hash := sha1.Sum(store)
	hashStr := fmt.Sprintf("%x", hash)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error hashing file \n")
		os.Exit(1)
	}

	if write {
		dir := fmt.Sprintf(".git/objects/%s", hashStr[:2])
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
	path := fmt.Sprintf(".git/objects/%s/%s", dir, name)

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

	// Skip header
	nullIndex := bytes.IndexByte(decompressed, 0)
	if nullIndex == -1 {
		fmt.Fprintf(os.Stderr, "Invalid Git object format: missing null byte\n")
		os.Exit(1)
	}
	data = decompressed[nullIndex+1:]

	// Parse tree entries
	i := 0
	for i < len(data) {
		// Read mode
		modeEnd := bytes.IndexByte(data[i:], ' ')
		mode := string(data[i : i+modeEnd])
		i += modeEnd + 1

		// Read filename
		nameEnd := bytes.IndexByte(data[i:], 0)
		filename := string(data[i : i+nameEnd])
		i += nameEnd + 1

		// Read SHA (20 bytes)
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
	sha, err := writeTree(".")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error writing tree: %s\n", err)
		os.Exit(1)
	}
	fmt.Println(sha)
}

func writeTree(dir string) (string, error) {
	entries := []byte{}
	files, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}

	for _, f := range files {
		if f.Name() == ".git" {
			continue
		}
		fullPath := filepath.Join(dir, f.Name())
		var mode string
		var sha string

		if f.IsDir() {
			mode = "40000"
			sha, err = writeTree(fullPath)
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

		entry := fmt.Sprintf("%s %s", mode, f.Name())
		entryBytes := []byte(entry)
		entryBytes = append(entryBytes, 0)

		shaRaw, _ := hex.DecodeString(sha)
		entryBytes = append(entryBytes, shaRaw...)

		entries = append(entries, entryBytes...)
	}

	header := fmt.Sprintf("tree %d\x00", len(entries))
	store := append([]byte(header), entries...)
	hash := sha1.Sum(store)
	sha := fmt.Sprintf("%x", hash)

	dirName := sha[:2]
	fileName := sha[2:]
	objPath := fmt.Sprintf(".git/objects/%s/%s", dirName, fileName)

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

	return sha, nil
}

func hashAndWriteBlob(filename string) (string, error) {
	content, err := os.ReadFile(filename)
	if err != nil {
		return "", err
	}

	header := fmt.Sprintf("blob %d\x00", len(content))
	store := append([]byte(header), content...)

	hash := sha1.Sum(store)
	sha := fmt.Sprintf("%x", hash)

	dir := sha[:2]
	name := sha[2:]
	path := fmt.Sprintf(".git/objects/%s/%s", dir, name)

	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.MkdirAll(fmt.Sprintf(".git/objects/%s", dir), 0755); err != nil {
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

	commitSha := os.Args[2]
	data := readObject(commitSha)

	// Strip header
	nullIndex := bytes.IndexByte(data, 0)
	body := data[nullIndex+1:]

	lines := bytes.Split(body, []byte("\n"))
	if len(lines) < 1 || !bytes.HasPrefix(lines[0], []byte("tree ")) {
		fmt.Fprintf(os.Stderr, "Invalid commit object\n")
		os.Exit(1)
	}
	treeSha := string(bytes.TrimPrefix(lines[0], []byte("tree ")))

	// First clean the current directory except for .git folder
	cleanCurrentDirectory()

	// Now restore the tree
	restoreTree(treeSha, ".")

	fmt.Printf("Restored to commit %s\n", commitSha)
}

func cleanCurrentDirectory() {
	// Get all entries in current directory
	entries, err := os.ReadDir(".")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading current directory: %s\n", err)
		os.Exit(1)
	}

	// Remove all files and directories except .git
	for _, entry := range entries {
		if entry.Name() == ".git" {
			continue
		}

		if err := os.RemoveAll(entry.Name()); err != nil {
			fmt.Fprintf(os.Stderr, "Error removing %s: %s\n", entry.Name(), err)
		}
	}
}

func readObject(sha string) []byte {
	dir, name := sha[:2], sha[2:]
	path := fmt.Sprintf(".git/objects/%s/%s", dir, name)

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

func restoreTree(treeSha string, pathPrefix string) {
	data := readObject(treeSha)

	// Strip header
	nullIndex := bytes.IndexByte(data, 0)
	body := data[nullIndex+1:]

	i := 0
	for i < len(body) {
		// parse mode
		modeEnd := bytes.IndexByte(body[i:], ' ')
		mode := string(body[i : i+modeEnd])
		i += modeEnd + 1

		// parse filename
		nameEnd := bytes.IndexByte(body[i:], 0)
		name := string(body[i : i+nameEnd])
		i += nameEnd + 1

		// parse sha (20 bytes)
		sha := fmt.Sprintf("%x", body[i:i+20])
		i += 20

		fullPath := filepath.Join(pathPrefix, name)

		if mode == "100644" {
			content := readObject(sha)
			// strip header
			nullIndex := bytes.IndexByte(content, 0)
			fileContent := content[nullIndex+1:]

			_ = os.MkdirAll(pathPrefix, 0755)
			if err := os.WriteFile(fullPath, fileContent, 0644); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to write %s: %s\n", fullPath, err)
			}
		} else if mode == "40000" || mode == "040000" {
			// Ensure the directory exists before recursively restoring it
			if err := os.MkdirAll(fullPath, 0755); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to create directory %s: %s\n", fullPath, err)
				continue
			}
			restoreTree(sha, fullPath)
		}
	}
}
