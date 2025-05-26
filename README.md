# Synchronization-of-Heterogeneous-Systems
Synchronization of heterogenous systems considering Hive, PostgreSQL and MongoDB. 

## PostgreSQL setup:

```bash
sudo service postgresql start
```
```bash
sudo -i -u postgres
```

Now to access psql shell:
```bash
psql
```
```bash
ALTER USER postgres WITH PASSWORD 'your_new_password';
```

## Introduction:
- This project provides a multithreaded simulator for database operations (SET, GET, MERGE, FULL_SYNC) across multiple database handlers. It parses an input script of operations, maintains in-memory caches per database, writes operation logs, and asynchronously synchronizes state to actual database handlers.

## Features:
- Precise timestamping with nanosecond resolution in UTC
- Concurrent sync to multiple databases via ThreadPoolExecutor
- Incremental merges between database caches using a merge module
- Full synchronization block to merge all caches in bulk
- Operation logging to per-database log files
- Cache-first GET to reduce actual database calls

## Code Overview:
### Entry Point: parse_testcase_file_multithreading:
- Reads each line, strips whitespace, skips empty lines.
- Detects optional numeric timestamp prefix (ignored for ordering).
- Matches operations via regular expressions: SET, GET, MERGE, FULL_SYNC.

### Timestamp Generation:
- get_precise_timestamp(): uses time.time_ns() to get nanosecond-precision UTC timestamps formatted as YYYY-MM-DD HH:MM:SS.NNNNNNNNNZ.

### Line Parsing:
- SET: extracts db_name, (student_id,course_id), grade.
- GET: extracts db_name, (student_id,course_id).
- MERGE: extracts source and target database names.
- FULL_SYNC: triggers a merge of all caches into one then redistributes.

### In-memory Caches & Logs:
- Dynamic global caches: e.g., MONGODB_cache, POSTGRESQL_cache.
- On SET, store [timestamp, grade] in cache and append log to oplogs.<db>.
- On GET, check cache first; if missing, delegate to handler.get(), then log the returned value.

### Full Sync & Merge Operations:
- FULL_SYNC: pairwise merge all caches into the first, then merge back, ensuring all caches converge.
- MERGE: incremental merge of two caches, then schedule asynchronous sync of the target cache to its real handler.

### Thread Pool for Asynchronous Sync:
- A ThreadPoolExecutor with up to 50 workers.
- Each required sync(handler, snapshot) call is submitted as a future.
- At the end, a final sync of each cache to its handler is submitted, then executor.shutdown(wait=True) to await all tasks.

## Architecture:
### Component Diagram:
```mermaid
flowchart LR
  subgraph Parser
    A[Read & Parse File] --> B[Regex Match Ops]
  end
  subgraph Cache & Log
    C[Cache] --> D[Log File: oplogs.db]
  end
  subgraph Merge & Sync
    E[merge(cache1, cache2)] --> F[ThreadPoolExecutor]
    F --> G[sync(handler, snapshot)]
  end

  A -->|"SET / GET / MERGE / FULL_SYNC"| C
  C -->|"write log"| D
  C -->|"on MERGE/FULL_SYNC"| E
  G -->|"apply to real DB"| H[(Database Handlers)]
mermaid
