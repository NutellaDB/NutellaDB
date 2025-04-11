# NutellaDB – B‑tree Storage Layer Documentations

## Table of contents

- [NutellaDB – B‑tree Storage Layer Documentations](#nutelladb-btree-storage-layer-documentations)
  - [Table of contents](#table-of-contents)
  - [Big picture](#big-picture)
  - [On‑disk layout](#ondisk-layout)
  - [File‑by‑file walkthrough](#filebyfile-walkthrough)
    - [`btree.go`](#btreego)
    - [`types.go`](#typesgo)
    - [`fs_handler.go`](#fs_handlergo)
    - [`kv_insert.go`](#kv_insertgo)
    - [`kv_find.go`](#kv_findgo)
    - [`kv_update.go`](#kv_updatego)
    - [`kv_delete.go`](#kv_deletego)
    - [`kv_repair` and helpers](#kv_repair-and-helpers)
    - [`utils.go`](#utilsgo)
  - [Concurrency \& locking](#concurrency--locking)
  - [Extending the tree](#extending-the-tree)
  - [Troubleshooting tips](#troubleshooting-tips)

---

## Big picture

The **B‑tree** implementation is the low‑level storage engine behind
NutellaDB collections. It is a _persistent_ B‑tree—every node is
serialised as a JSON file on disk so the structure can be re‑opened
between process runs. A single collection lives inside its own
`<collection>/pages/` directory.

Key design decisions:

| Decision                               | Rationale                                                                          |
| -------------------------------------- | ---------------------------------------------------------------------------------- |
| **JSON** for node files                | Easy to debug with any text editor; human‑readable snapshots.                      |
| **Lock‑free reads, coarse write lock** | Only writers mutate metadata; readers only need to walk cached nodes.              |
| **Pluggable order** (`t ≥ 3`)          | Lets callers choose the node fan‑out when creating a collection.                   |
| **Lazy caching**                       | Nodes are kept in memory only after first access; evicted when the tree is closed. |
| **Self‑healing**                       | `RepairTree` can rebuild missing nodes and fix most on‑disk corruptions.           |

---

## On‑disk layout

```bash
files/
  └─ <dbID>/
       └─ <collection>/
            └─ pages/
                 ├─ metadata.json   # serialised *BTree struct*
                 ├─ page_1.json     # root node
                 ├─ page_2.json
                 └─ ...
```

_`metadata.json`_ contains the **root ID**, current **next node ID**,
`order`, and other bookkeeping fields. Each `page_N.json` holds a
serialised `Node` (keys + child IDs).

---

## File‑by‑file walkthrough

### `btree.go`

_Entry point_ – constructors and lifecycle helpers.

- **`NewBTree`** – creates an empty tree, initialises the root node, and
  persists `metadata.json`.
- **`LoadBTree`** – opens an existing tree by reading `metadata.json` and
  populating an in‑memory cache.
- **`saveMetadata` / `Close`** – guarantee that the on‑disk metadata is
  always consistent.

> Tip: `NewBTree` never overwrites existing data; callers must ensure the
> destination directory is empty.

### `types.go`

Holds the _data structures_:

- **`KeyValue`** – thin wrapper around a string key and an arbitrary
  Go `interface{}` value (serialised via `encoding/json`).
- **`Node`** – in‑memory representation of one B‑tree node. The `Keys`
  slice is always kept **sorted**.
- **`BTree`** – top‑level object. The `metadata` field is a RW‑mutex
  used only for the `NextID` allocator and `RootID` swaps.

### `fs_handler.go`

Low‑level persistence helpers:

- **`saveNode`** – JSON‑encodes a node, writes it to
  `page_<id>.json`, and caches it.
- **`loadNode`** – reads from disk _unless_ the node is already in the
  in‑memory cache.
- **`deleteNode`** – removes both the file and the cache entry.

These helpers are the only code that touches the filesystem.

### `kv_insert.go`

Implements **insertion**:

1. `Insert` is the public API – handles root‑splitting.
2. `splitChild` – classic B‑tree split algorithm (middle key promoted to
   parent).
3. `insertNonFull` – walks downwards until it finds a leaf that has room.

Edge cases handled:

- Upsert (existing key updates its value).
- Splitting full children on the way down.

### `kv_find.go`

Read‑only search (`Find`, `findInNode`). Purely recursive and never
modifies the tree, so it takes **no locks**.

### `kv_update.go`

Updates a key _only if it exists_ – otherwise returns `false` so the
caller can decide to insert instead.

### `kv_delete.go`

Full delete algorithm including:

- **`ensureMinKeys`** – makes sure a child has at least `t` keys by
  borrowing from siblings or merging.
- **`mergeNodes`** – merges two siblings and pulls down the separator
  key from the parent.
- **`getPredecessor`** – finds the largest key in the left subtree.
- Self‑healing checks – if a node file is missing on disk, the algorithm
  logs a warning and tries to continue.

### `kv_repair` and helpers

Deletion code brings its own _repair_ utilities:

- **`RepairTree`** – entry point that walks the entire tree and rebuilds
  invalid references.
- **`repairNode`** – removes dangling child IDs, converts empty internal
  nodes into leaves, etc.

### `utils.go`

Thin convenience wrappers that call the core API and `log.Fatalf` on
errors. They are mainly used by higher‑level packages (`database` &
`server`).

---

## Concurrency & locking

- The **metadata lock** (`bt.metadata`) is used only for **`NextID`
  allocation** and **root replacement**. All other mutations rely on
  single‑threaded access by the caller.
- Node‑level operations do **not** use fine‑grained locks. If you plan
  to add concurrent writers you’ll need a higher‑level transaction or
  page‑latch system.

---

## Extending the tree

| Task                        | Where to start                                               |
| --------------------------- | ------------------------------------------------------------ |
| **Range scans / iteration** | add `Next()` / `Prev()` helpers in `kv_find.go`.             |
| **Bulk load**               | implement a bottom‑up builder that bypasses `insertNonFull`. |
| **Custom key types**        | replace `string` with generics (Go 1.22+).                   |
| **Compression**             | swap JSON for a binary encoding in `fs_handler.go`.          |

---

## Troubleshooting tips

- **Missing node file** → `find` / `delete` will print a warning and
  attempt auto‑repair. Run `RepairTree()` manually if the tree looks
  inconsistent.
- **Performance** → the most common culprit is tiny `order` (fan‑out).
  Use `order >= 64` for realistic workloads.
- **Disk usage** → remember each key/value is stored _verbatim_ in JSON.
  Consider storing only pointers or enabling compression.

---
