# Command Reference: REST API Server

## Table of Contents

- [Command Reference: REST API Server](#command-reference-rest-api-server)
  - [Table of Contents](#table-of-contents)
  - [Core Database Commands](#core-database-commands)
    - [Create a New Database](#create-a-new-database)
    - [Create a New Collection](#create-a-new-collection)
  - [Data Operations](#data-operations)
    - [Insert Key-Value Pair](#insert-key-value-pair)
    - [Find Key](#find-key)
    - [Update Key-Value Pair](#update-key-value-pair)
    - [Delete Key](#delete-key)
  - [Version Control Commands](#version-control-commands)
    - [Initialize Version Control](#initialize-version-control)
    - [Commit Changes](#commit-changes)
    - [Restore to a Previous Commit](#restore-to-a-previous-commit)
    - [Pack Objects](#pack-objects)

---

## Core Database Commands

### Create a New Database

- **Endpoint:** `/api/create-db`
- **Method:** `POST`
- **Description:** Creates a new database. The database is created with a specified `dbID` and stored under `./files/db_[id]`.
- **Example Usage:**

```bash
curl -X POST localhost:3000/api/create-db \
-H 'Content-Type: application/json' \
-d '{"dbID":"db_x"}'
```

### Create a New Collection

- **Endpoint:** `/api/create-collection`
- **Method:** `POST`
- **Description:** Creates a new collection within a specified database. You must provide the `dbID`, a collection `name`, and the B-tree `order` (integer ≥ 3).
- **Example Usage:**

```bash
curl -X POST localhost:3000/api/create-collection \
-H 'Content-Type: application/json' \
-d '{"dbID":"db_x","name":"fruits","order":3}'
```

---

## Data Operations

### Insert Key-Value Pair

- **Endpoint:** `/api/insert`
- **Method:** `POST`
- **Description:** Inserts a key-value pair into the specified collection of a database.
- **Example Usage:**

```bash
curl -X POST localhost:3000/api/insert \
-H 'Content-Type: application/json' \
-d '{"dbID":"db_x","collection":"fruits","key":"apple","value":"red"}'
```

### Find Key

- **Endpoint:** `/api/find`
- **Method:** `GET`
- **Description:** Searches for a specified key in a collection and returns the associated value.
- **Example Usage:**

```bash
curl "localhost:3000/api/find?dbID=db_x&collection=fruits&key=apple"
```

### Update Key-Value Pair

- **Endpoint:** `/api/update`
- **Method:** `POST`
- **Description:** Updates an existing key with a new value in the specified collection.
- **Example Usage:**

```bash
curl -X POST localhost:3000/api/update \
-H 'Content-Type: application/json' \
-d '{"dbID":"db_x","collection":"fruits","key":"apple","new_value":"green"}'
```

### Delete Key

- **Endpoint:** `/api/delete`
- **Method:** `POST`
- **Description:** Deletes a key and its corresponding value from the specified collection.
- **Example Usage:**

```bash
curl -X POST localhost:3000/api/delete \
-H 'Content-Type: application/json' \
-d '{"dbID":"db_x","collection":"fruits","key":"apple"}'
```

---

## Version Control Commands

### Initialize Version Control

- **Endpoint:** `/api/init`
- **Method:** `POST`
- **Description:** Initializes version control for a specific database, setting up the `.nut` directory and necessary internal structures.
- **Example Usage:**

```bash
curl -X POST localhost:3000/api/init \
-H 'Content-Type: application/json' \
-d '{"dbID":"db_x"}'
```

### Commit Changes

- **Endpoint:** `/api/commit-all`
- **Method:** `POST`
- **Description:** Recursively hashes files in the database (excluding certain directories) to generate a tree object and commit object, which are stored in `snapshots.json`.
- **Example Usage:**

```bash
curl -X POST localhost:3000/api/commit-all \
-H 'Content-Type: application/json' \
-d '{"dbID":"db_x","message":"first"}'
```

### Restore to a Previous Commit

- **Endpoint:** `/api/restore`
- **Method:** `POST`
- **Description:** Displays available snapshots (commit history) and reverts the working directory to a specified previous commit.
- **Example Usage:**

```bash
curl -X POST localhost:3000/api/restore \
-H 'Content-Type: application/json' \
-d '{"dbID":"db_x","commitHash":"<commit_hash>"}'
```

### Pack Objects

- **Endpoint:** `/api/pack`
- **Method:** `POST`
- **Description:** Compresses loose objects into a packfile to optimize storage.
- **Example Usage:**

```bash
curl -X POST localhost:3000/api/pack \
-H 'Content-Type: application/json' \
-d '{"dbID":"db_x"}'
```
