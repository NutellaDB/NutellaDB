# CLI Command Reference

## Table of Contents
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
- **Command**: `create-db`  
- **Description**: Creates a new database at the specified location.
- **Example Usage**:
```
go run . create-db --dbID=db_x
```

### Create a New Collection
- **Command**: `create-collection`  
- **Description**: Creates a collection in a given database with a specified B-tree order (≥ 3).
- **Required Flags**:
  - `--dbID` : Specifies the database ID.
  - `--name` : Specifies the name of the collection.
  - `--order` : Specifies the B-tree order.
- **Example Usage**:
```
go run . create-collection --dbID=db_x --name=fruits --order=3
```

---

## Data Operations

### Insert Key-Value Pair
- **Command**: `insert`  
- **Description**: Inserts a key-value pair into the specified collection.
- **Example Usage**:
```
go run . insert --dbID=db_x --collection=fruits --key=apple --value=red
```

### Find Key
- **Command**: `find`  
- **Description**: Searches for a key and displays the associated value.
- **Example Usage**:
```
go run . find --dbID=db_x --collection=fruits --key=apple
```

### Update Key-Value Pair
- **Command**: `update`  
- **Description**: Updates the value of an existing key in the specified collection.
- **Example Usage**:
```
go run . update --dbID=db_x --collection=fruits --key=apple --new_value=green
```

### Delete Key
- **Command**: `delete`  
- **Description**: Deletes a key and its corresponding value from the collection.
- **Example Usage**:
```
go run . delete --dbID=db_x --collection=fruits --key=apple
```

---

## Version Control Commands

### Initialize Version Control
- **Command**: `init`  
- **Description**: Initializes version control in the database and creates the .nutella structure.
- **Example Usage**:
```
go run . init --dbID=db_x
```

### Commit Changes
- **Command**: `commit-all`  
- **Description**: Commits the current state of the database with a commit message.
- **Example Usage**:
```
go run . commit-all --dbID=db_x --message="first"
```

### Restore to a Previous Commit
- **Command**: `restore`  
- **Description**: Reverts the database to the specified commit hash.
- **Example Usage**:
```
go run . restore --dbID=db_x --commitHash=<commit_hash>
```

### Pack Objects
- **Command**: `pack`  
- **Description**: Compresses all loose objects into a single packfile to optimize storage.
- **Example Usage**:
```
go run . pack --dbID=db_x
```
