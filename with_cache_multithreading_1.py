import re
import json
import os
import csv
import threading
from collections import defaultdict
from MongoDB_connect import MongoDBHandler
from postgresql_connector import PostgreSQLHandler
from hive import Hive

# Handlers for each database
mongo_handler = MongoDBHandler()
postgre_handler = PostgreSQLHandler()
hive_handler = Hive("student_grades", "localhost", 10000, "prat", "CUSTOM", "pc02@December")

# Caches: dict[key] = (timestamp, value)
mongo_logs = defaultdict(lambda: (0, ""))
hive_logs = defaultdict(lambda: (0, ""))
postgresql_logs = defaultdict(lambda: (0, ""))

# Map name to cache
db_logs_map = {
    'MONGODB': mongo_logs,
    'HIVE': hive_logs,
    'POSTGRESQL': postgresql_logs
}

# Primary keys list
primary_keys = []

# Initialize from CSV
def init_primary_keys(csv_file_path):
    with open(csv_file_path, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            key = (row['student-ID'].strip(), row['course-id'].strip())
            primary_keys.append(key)
            # Initialize caches
            for logs in db_logs_map.values():
                logs[key] = (0, "")

# Sync function: runs in separate thread to persist cache to DB
def sync(cache_copy: dict, db_name: str):
    """
    Synchronize the given cache snapshot with the actual database.
    """
    handler = {
        'MONGODB': lambda k, v, t: mongo_handler.set("university_db", "grades_of_students", k, v, t),
        'POSTGRESQL': lambda k, v, t: postgre_handler.set("student_course_grades", k, v, t),
        'HIVE': lambda k, v, t: hive_handler.update_data(k, v)
    }[db_name]

    for key, (timestamp, value) in cache_copy.items():
        if timestamp > 0:
            handler(key, value, timestamp)

# SET operation: updates cache and underlying DB
def db_set(db_name: str, pk: tuple, value: str, ts: int):
    if pk not in primary_keys:
        print(f"{pk} not in primary keys.")
        return
    # Update cache
    db_logs_map[db_name][pk] = (ts, value)
    # Persist immediately for non-merge or as needed
    if db_name == 'HIVE':
        hive_handler.update_data(pk, value)
    elif db_name == 'MONGODB':
        mongo_handler.set("university_db", "grades_of_students", pk, value, ts)
    elif db_name == 'POSTGRESQL':
        postgre_handler.set("student_course_grades", pk, value, ts)

# GET operation: check cache first, fallback to DB
def db_get(db_name: str, pk: tuple) -> str:
    if pk not in primary_keys:
        print(f"{pk} not in primary keys.")
        return None
    ts, val = db_logs_map[db_name][pk]
    if ts > 0:
        return val
    # fallback
    if db_name == 'HIVE':
        return hive_handler.select_data("student_grades", pk)
    elif db_name == 'MONGODB':
        return mongo_handler.get("university_db", "grades_of_students", pk)
    elif db_name == 'POSTGRESQL':
        return postgre_handler.get("student_course_grades", pk)
    return None

# Generic merge: merges cache B into A, then triggers sync(A)
def generic_merge(db1: str, db2: str):
    logs1 = db_logs_map[db1]
    logs2 = db_logs_map[db2]
    # Merge based on timestamp
    for pk in primary_keys:
        ts1, _ = logs1[pk]
        ts2, val2 = logs2[pk]
        if ts2 > ts1:
            logs1[pk] = (ts2, val2)
    # After updating cache A, spawn sync thread with snapshot
    cache_snapshot = logs1.copy()
    threading.Thread(target=sync, args=(cache_snapshot, db1), daemon=True).start()

# Parse and execute operations from testcase file
def parse_testcase_file(file_path, gc_start=0):
    gc = gc_start
    # load existing gc
    if os.path.exists('gc.txt'):
        with open('gc.txt', 'r') as f:
            gc = int(f.read())
    with open(file_path, 'r') as f:
        for idx, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            timestamp = idx + 1 + gc
            db_timestamp = None
            # handle optional leading timestamp
            if ',' in line:
                parts = line.split(',', 1)
                if parts[0].strip().isdigit():
                    db_timestamp = int(parts[0].strip())
                    line = parts[1].strip()
            # Determine operation
            if 'MERGE' in line:
                db1, db2 = re.match(r'(\w+)\.MERGE\((\w+)\)', line).groups()
                generic_merge(db1, db2)
            elif 'SET' in line:
                db1, sid, cid, val = re.match(r'(\w+)\.SET\(\(([^,]+),([^)]+)\),\s*([^)]+)\)', line).groups()
                db_set(db1, (sid, cid), val, timestamp)
            elif 'GET' in line:
                db1, sid, cid = re.match(r'(\w+)\.GET\(([^,]+),([^)]+)\)', line).groups()
                result = db_get(db1, (sid, cid))
                print(f"{db1}.GET(({sid},{cid})) = {result}")
    # persist gc
    with open('gc.txt', 'w') as f:
        f.write(str(timestamp))

if __name__ == '__main__':
    # initialize keys
    init_primary_keys('student_course_grades.csv')
    parse_testcase_file('example_testcase_3.in')