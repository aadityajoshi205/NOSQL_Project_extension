import re
import os
import csv
import concurrent.futures
from MongoDB_connect import MongoDBHandler
from postgresql_connector import PostgreSQLHandler
from hive import Hive

# Handlers for each database
mongo_handler = MongoDBHandler()
postgre_handler = PostgreSQLHandler()
hive_handler = Hive("student_grades", "localhost", 10000, "prat", "CUSTOM", "pc02@December")

# Caches: dict[key] = (timestamp, value)
mongo_logs = {}
hive_logs = {}
postgresql_logs = {}

# Map name to cache
db_logs_map = {
    'MONGODB': mongo_logs,
    'HIVE': hive_logs,
    'POSTGRESQL': postgresql_logs
}

# Primary keys list
primary_keys = set()

# Thread pool executor and future tracking
executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
sync_futures = []

def init_primary_keys(csv_file_path):
    """
    Load valid primary keys from the CSV into the primary_keys set.
    We no longer pre-populate any cache entries here.
    """
    with open(csv_file_path, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            key = (row['student-ID'].strip(), row['course-id'].strip())
            primary_keys.add(key)

def sync(cache_snapshot: dict, db_name: str):
    """
    Persist a snapshot of the cache to the real database.
    Runs in the thread pool.
    """
    handler = {
        'MONGODB': lambda k, v, t: mongo_handler.set("university_db", "grades_of_students", k, v, t),
        'POSTGRESQL': lambda k, v, t: postgre_handler.set("student_course_grades", k, v, t),
        'HIVE': lambda k, v, t: hive_handler.update_data(k, v),
    }[db_name]

    for key, (timestamp, value) in cache_snapshot.items():
        if timestamp > 0:
            handler(key, value, timestamp)

def db_set(db_name: str, pk: tuple, value: str, ts: int):
    """
    SET: update the in-memory cache, then immediately persist to the underlying DB.
    """
    if pk not in primary_keys:
        print(f"{pk} not in primary keys.")
        return

    # Update our cache
    db_logs_map[db_name][pk] = (ts, value)

    # Persist immediately for non-merge operations
    if db_name == 'HIVE':
        hive_handler.update_data(pk, value)
    elif db_name == 'MONGODB':
        mongo_handler.set("university_db", "grades_of_students", pk, value, ts)
    else:  # POSTGRESQL
        postgre_handler.set("student_course_grades", pk, value, ts)

def db_get(db_name: str, pk: tuple) -> str:
    """
    GET: if the key is in our cache, return it.
    Otherwise fetch from the real database.
    """
    if pk not in primary_keys:
        print(f"{pk} not in primary keys.")
        return None

    cache = db_logs_map[db_name]
    if pk in cache:
        return cache[pk][1]

    # otherwise, fallback to the real DB
    if db_name == 'HIVE':
        return hive_handler.select_data("student_grades", pk)
    elif db_name == 'MONGODB':
        return mongo_handler.get("university_db", "grades_of_students", pk)
    else:  # POSTGRESQL
        return postgre_handler.get("student_course_grades", pk)

def generic_merge(db1: str, db2: str):
    """
    MERGE B → A: for every key in B's cache:
      - If key is not in A, copy it.
      - If key is in A, but B has newer timestamp, overwrite.
    Then schedule background sync of A.
    """
    logs1 = db_logs_map[db1]
    logs2 = db_logs_map[db2]

    for pk, (ts2, val2) in logs2.items():
        ts1, _ = logs1.get(pk, (0, None))
        if pk not in logs1 or ts2 > ts1:
            logs1[pk] = (ts2, val2)

    # snapshot and schedule background sync
    snapshot = dict(logs1)
    future = executor.submit(sync, snapshot, db1)
    sync_futures.append(future)

def parse_testcase_file(file_path, gc_start=0):
    """
    Read the testcase.in, line by line, and execute GET / SET / MERGE.
    Maintains a global counter (gc) for timestamps.
    """
    gc = gc_start
    if os.path.exists('gc.txt'):
        with open('gc.txt', 'r') as f:
            gc = int(f.read())

    with open(file_path, 'r') as f:
        for idx, line in enumerate(f):
            raw = line.strip()
            if not raw:
                continue

            timestamp = idx + 1 + gc
            # drop leading timestamp if present
            if ',' in raw:
                head, rest = raw.split(',', 1)
                if head.strip().isdigit():
                    raw = rest.strip()

            if '.MERGE(' in raw:
                db1, db2 = re.match(r'(\w+)\.MERGE\((\w+)\)', raw).groups()
                generic_merge(db1, db2)

            elif '.SET(' in raw:
                db1, sid, cid, val = re.match(
                    r'(\w+)\.SET\(\(([^,]+),([^)]+)\),\s*([^)]+)\)', raw
                ).groups()
                db_set(db1, (sid, cid), val, timestamp)

            elif '.GET(' in raw:
                db1, sid, cid = re.match(
                    r'(\w+)\.GET\(([^,]+),([^)]+)\)', raw
                ).groups()
                result = db_get(db1, (sid, cid))
                print(f"{db1}.GET(({sid},{cid})) = {result}")

    # persist the new gc value
    with open('gc.txt', 'w') as f:
        f.write(str(timestamp))

if __name__ == '__main__':
    try:
        init_primary_keys('student_course_grades.csv')
        parse_testcase_file('example_testcase_3.in')
    finally:
        # wait for all background syncs to finish before exiting
        executor.shutdown(wait=True)