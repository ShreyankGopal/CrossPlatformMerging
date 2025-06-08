# Cross-Platform Data Operation Handler and Logger

**Course:** DAS 839: NoSQL Systems  
**Submitted on:** April 29, 2025  

---

## Overview

This project implements a **unified data operation handler and logger** that supports:
- **MySQL**
- **Apache Pig**
- **MongoDB**

Users can issue commands in a unified JSON format to:
- Perform **GET** and **SET** operations across systems.
- Log every operation with simulated timestamps.
- **Merge data across systems** using a robust, conflict-aware synchronization logic.

---

## Components

### 1. MySQL Java Class
- Handles `SELECT` and `UPDATE` SQL queries.
- Logs operations to `SQLLog.jsonl`.

### 2. Pig Java Class
- Runs local Pig scripts to manipulate data from `grades.csv`.
- Logs operations to `PigLog.jsonl`.

### 3. MongoDB Java Class
- Executes `find()` and `updateOne()` queries on collections.
- Logs operations to `MongoLog.jsonl`.

---

## Test Cases

Operations are tested on a unified `Grades` schema:  
**`studentID`, `subjectCode`, `grade`**

### SET
- Inserts/updates grades for a student-subject pair across systems.

### GET
- Fetches grade records to verify read consistency.

### MERGE
- Propagates updates from one system to another (e.g., Mongo → Pig).

---

## Operation Log Format

Each operation is stored in `.jsonl` logs using a consistent format:
- `db`: Database system (MYSQL, PIG, MONGO)
- `op`: Type of operation (GET, SET, MERGE)
- `table`, `key attributes`, `key values`
- `column attributes`, `column values`
- `getColumns` (for GET)
- `time`: Simulated timestamp for chronological ordering

---

## Merge Mechanism

### Pointer Management

- **Six pointer pairs** maintain progress for each direction of merge:
  - Example: `Pig → MySQL`, `Mongo → Pig`, etc.
- Each merge updates only its relevant pointer pair.
- This ensures incremental merging, avoiding reprocessing older operations.

### Merge Execution Flow

1. **Identify new SET operations** in both `from` and `to` systems since the last merge.
2. Build hashmaps using:
   - Composite keys: `table + key attributes + column attributes`
   - Values: `(value, timestamp)`
3. **Compare records** from `from` and `to` systems:
   - If a key is **missing** in `to`, copy it.
   - If the value is **different**, retain the one with the higher timestamp.
   - If identical, skip.

4. Append only new/updated SET operations to the `to` system's oplog.

---
## 🔄 Consistency Tracking Across Systems

To maintain **eventual consistency** across the three database systems—**MySQL**, **Pig**, and **MongoDB**—we use a **merge-tracking state diagram** and a set of **Boolean flags**.

### 🔁 Merge State Diagram

Each directed edge (e.g., `mysql → pig`) represents that the destination system has incorporated all updates from the source system. These relationships are updated on every merge and form the basis for tracking data synchronization.

![Merge State Diagram]()

---

### 🧩 Flag Representation

Each edge in the diagram corresponds to a Boolean flag used in the system to track merge status:



| Edge | Boolean flag in code            |
|------|----------------------------------|
| m1   | `mysql_merged_with_pig`         |
| m2   | `pig_merged_with_mongo`         |
| m3   | `mongo_merged_with_mysql`       |
| m4   | `mysql_merged_with_mongo`       |
| m5   | `mongo_merged_with_pig`         |
| m6   | `pig_merged_with_mysql`         |

---

### 🧠 How It Works

- **After a SET operation**, all outbound flags from the modified system are set to `false` (others are outdated).
- **After a MERGE**, the corresponding `from → to` flag is set to `true`.
- **Transitivity Rule**:  
  If system C had already seen all updates from A (`C → A` is true), and A just merged into B (`A → B`),  
  then `C → B` is set to true as well.
- **Correctness Rule**:  
  If `B → C` is true but B just received new updates from A (`A → B`), and C hasn't seen A's updates,  
  then `B → C` is set to `false`.

---

### ✅ Full Synchronization

Once **all six flags are true**, the system is considered fully synchronized:
- **Operation logs are flushed**
- **Merge pointers reset**

This ensures that **no redundant merges** happen and the three systems operate in a consistent, traceable state.

## Merge Properties

Our merge implementation satisfies key principles of distributed systems:

### Idempotence
- Merging the same systems repeatedly causes no extra effect.

### Commutativity
- Merge order does not matter: `merge(A,B) == merge(B,A)`

### Associativity
- Grouping of merge operations does not affect the final result:
  - `merge(A, merge(B, C)) == merge(merge(A, B), C)`

These properties guarantee **eventual consistency** across systems in a decentralized, fault-tolerant manner.

---

## 📁 Logs
- `SQLLog.jsonl`
- `PigLog.jsonl`
- `MongoLog.jsonl`

---

## Final Note

This system offers a **modular, fault-tolerant, and auditable interface** for handling, tracking, and synchronizing operations across MySQL, Pig, and MongoDB — reflecting real-world database interoperability in distributed environments.

