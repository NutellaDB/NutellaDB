# Nutella Documentation

## Table of Contents
- [Overview](#overview)
- [Introduction](#introduction)
- [Benefits](#benefits)
- [Core Concepts](#core-concepts)
- [Technical Details](#technical-details)
  - [Object Types and Structure](#object-types-and-structure)
  - [Delta Format](#delta-format)
  - [Storage Structure](#storage-structure)
- [Advanced Features](#advanced-features)
  - [Delta Compression](#delta-compression)
  - [.nutellaignore File](#nutellaignore-file)
  - [In-Memory LRU Cache](#in-memory-lru-cache)

---

## Overview
Nutella is a Git-inspired version control system specifically designed for a key-value database. It tracks changes by storing only the differences (delta compression) rather than complete snapshots. This documentation provides an outline of Nutella’s functionalities, commands, and technical details.

---

## Introduction
Nutella is a specialized version control system for key-value databases that brings Git-like capabilities to database management. It allows tracking, versioning, and reverting changes to database structures and content.

---

### Benefits
- **History Tracking**: Maintain a complete record of database changes  
- **Point-in-Time Recovery**: Restore to previous database states when needed  
- **Change Management**: Document and control database modifications  
- **Safe Experimentation**: Test changes with confidence knowing you can revert  
- **Storage Efficiency**: Minimize disk usage through delta compression  
- **Auditability**: Keep comprehensive logs of who changed what and when  

By applying version control principles to database management, Nutella enables developers and administrators to manage their data with greater confidence and flexibility.

---

## Core Concepts
- **Version Control**: Manage snapshots and changes in your database.  
- **Delta Compression**: Save space by storing only the differences between versions.  
- **Object Types**: Understand the different objects:
  - **Blob**: Represents file content.  
  - **Tree**: Represents directory structure.  
  - **Commit**: Represents a snapshot of the database.  
  - **Delta**: Stores differences between objects.  

---

## Technical Details

### Object Types and Structure
- **Blob**: Represents file content.  
- **Tree**: Represents the directory structure.  
- **Commit**: Represents snapshots of the current state.  
- **Delta**: Contains differences between objects.  

### Delta Format
- **Format Structure:**  
  - 4 bytes: Size of source (base) content.  
  - 4 bytes: Size of target content.  
  - Followed by a series of instructions (copy or insert operations).  

### Storage Structure
- **Directory Organization:**  
  All objects are stored in the `.nutella/objects` directory, organized by the first two characters of their SHA-1 hash.

---

## Advanced Features

### Delta Compression
- **Description:**  
  Delta compression is used in Nutella to reduce storage overhead by storing only the differences between similar objects rather than the full contents. When a new object is added, the system searches for an existing object that is similar. If a match is found, Nutella computes the difference (delta) between the new object and the identified base object and saves only that delta. This minimizes redundancy and disk usage while preserving the full data when needed.

- **Technical Details:**  
  - **Base Object Identification:**  
    When storing a new object, Nutella scans the stored objects (located in the `.nutella/objects` directory) to find a similar file. It uses a simplified similarity check based on file size and content samples. If a similar object is found, it is used as the base for delta calculation.  
  - **Computing the Delta:**  
    The system computes a delta by determining which sections of the new object match the base object (copy operations) and which parts are new (insert operations). The delta format includes:
    - A 4-byte field for the size of the base (source) content.  
    - A 4-byte field for the size of the target (new) content.  
    - A series of delta operations:
      - **Copy Operations:** Reuse blocks from the base object.  
      - **Insert Operations:** Add new content that is not present in the base object.  
  - **Storage and Retrieval:**  
    The computed delta is compressed and stored as a delta object. During read operations, Nutella automatically identifies delta objects and applies them to the corresponding base object to reconstruct the complete file, ensuring transparency for the end user.

### .nutellaignore File
- **Description:**  
  The `.nutellaignore` file lets you define patterns for files and directories that should be excluded from version control. By using `.nutellaignore`, you can prevent temporary files, build artifacts, log files, and other non-essential or auto-generated files from being tracked or included in snapshots.

- **Usage:**  
  - **Creating the File:**  
    Create a file named `.nutellaignore` in the root directory of your repository.  
  - **Defining Patterns:**  
    List the file or directory patterns you want to exclude. Each pattern should be on a separate line. For instance:
    ```
    # Ignore log files
    *.log

    # Ignore temporary files
    *.tmp

    # Ignore build directories
    /build
    ```
  These patterns are used by Nutella when scanning the directory for files to include in commits or tree snapshots.

- **Integration with Nutella:**  
  During commit operations (for example, when creating a tree object for snapshotting), Nutella automatically reads the `.nutellaignore` file and excludes any matching files or directories. Ensure that the patterns accurately reflect your project's folder structure and unwanted files to optimize storage and performance.

### In-Memory LRU Cache
- **Description:**  
  Nutella includes a Least Recently Used (LRU) caching system to optimize repeated key-value lookups and storage. The cache can run fully in-memory and uses eviction strategies to ensure it remains within a configured maximum size. Optionally, it can persist the in-memory cache to disk (`cache.json`) to maintain state between executions.

- **Key Features:**  
  - Multi-collection support  
  - LRU-based eviction policy  
  - Persistent storage via JSON  
  - Thread-safe operations using read-write locks  
  - Supports insert, update, delete, and find operations

- **Structure:**  
  - **CacheItem**: Stores each entry with collection, key, and value.  
  - **Cache**: The main struct with maps for fast lookup and a doubly linked list for LRU tracking.  
  - **CacheMap**: Nested maps of collection to (key → element).  
  - **LruList**: Maintains ordering based on access to implement LRU eviction.

- **Operations:**
  - `InsertInCacheMemory(basepath, collection, key, value)`: Insert a new entry.
  - `FindInCacheMemory(basepath, collection, key)`: Retrieve an entry, moving it to the front.
  - `UpdateCacheInMemory(basepath, collection, key, value)`: Update value of existing key.
  - `DeleteFromCacheMemory(basepath, collection, key)`: Remove an entry.
  - `AddCollectionToMemory(basepath, collectionName)`: Add a new logical collection to the cache.
  - `GetAllCollections()`, `GetAllKeys(collection)`: Query current cache state.

- **Eviction Policy:**  
  When the total number of items across all collections exceeds `MaxSize`, the least recently used item (at the back of the list) is evicted. Evicted entries are removed from both the list and the nested maps.

- **Persistence:**  
  - The cache can be saved to disk (`cache.json`) using `SaveCache(basepath)` and later reloaded with `LoadCacheFromMemory(basepath)`.  
  - This is useful for maintaining cache state across sessions or server restarts.  

- **Configuration:**  
  - Toggle in-memory persistence via the global `IS_IN_MEMORY` flag.  
  - Maximum cache size can be customized via `SetMaxSize(n)` and queried via `GetMaxSize()`.

This caching system greatly improves efficiency for frequently accessed values and is an integral part of Nutella’s fast and responsive behavior.
