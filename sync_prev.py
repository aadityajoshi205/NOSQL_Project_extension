import re
import json
import csv
from collections import defaultdict
import json
from MongoDB_connect import MongoDBHandler
from postgresql_connector import PostgreSQLHandler
from hive import Hive


mongo_handler = MongoDBHandler()
postgre_handler = PostgreSQLHandler()
hive_handler = Hive("student_grades", "localhost", 10000, "prat", "CUSTOM", "pc02@December")
#hive_handler.create_table('student_course_grades.csv')

# Initialize 2D dictionaries using defaultdict
mongo_logs = defaultdict(lambda: (0, ""))
hive_logs = defaultdict(lambda: (0, ""))
postgresql_logs = defaultdict(lambda: (0, ""))

# Path to your CSV file
csv_file_path = 'student_course_grades.csv'  # Replace with your actual path

primary_keys = []

# Read the CSV and initialize logs
with open(csv_file_path, mode='r', newline='', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    
    for row in reader:
        student_id = row['student-ID'].strip()
        course_id = row['course-id'].strip()
        grade = row['grade'].strip()
        key = (student_id, course_id)

        primary_keys.append(key)

        # Initialize with timestamp 0 and value ""
        mongo_logs[key] = (0, grade)
        hive_logs[key] = (0, grade)
        postgresql_logs[key] = (0, grade)

db_logs_map = {
    'HIVE': hive_logs,
    'MONGODB': mongo_logs,
    'POSTGRESQL': postgresql_logs
}



def db_set(db_name: str, pk: tuple, value: str, ts: int):
    if pk not in primary_keys:
        print(f"{pk} not in primary keys.")
        return
    if(db_name=="POSTGRESQL"):
        postgre_handler.set("student_course_grades", pk, value, ts)
    elif(db_name=="MONGODB"):
        mongo_handler.set("university_db", "grades_of_students", pk, value, ts)
    else:
        hive_handler.update_data(pk, value)
    db_logs_map[db_name][pk] = (ts, value)

def read_oplogs(db: str):
    with open(f"oplogs.{db.lower()}", 'r') as file:
        print(f"Reading oplogs for {db}")
        for idx, line in enumerate(file):
            line = line.strip()
            print(f"Line {idx+1}: {line}")
            if not line:
                continue  # Skip empty lines

            db_timestamp = None
            operation = None
            db1 = db2 = student_id = course_id = grade = None

            # Check for leading timestamp (db_timestamp)
            if ',' in line:
                parts = line.split(',', 1)
                if parts[0].strip().isdigit():
                    db_timestamp = int(parts[0].strip())
                    line = parts[1].strip()

            # Parse SET
            if 'SET' in line:
                match = re.match(r'(\w+)\.SET\(\(([^,]+),([^)]+)\),\s*([^)]+)\)', line)
                if match:
                    db1, student_id, course_id, grade = match.groups()
                    operation = 'SET'

            # Parse GET
            elif 'GET' in line:
                match = re.match(r'(\w+)\.GET\(([^,]+),([^)]+)\)', line)
                if match:
                    db1, student_id, course_id = match.groups()
                    operation = 'GET'

            print(f"operation = {operation}")

            if operation == 'SET':
                print(f"SET: {db}({db_timestamp}) {student_id}, {course_id}, {grade}")
                db_set(db_name=db, pk=(student_id, course_id), value=grade, ts=db_timestamp)


def merge(db1:str, db2:str, ts:int)->list:
    """
    There are 2 approaches which can be used to update the timestamp after a merge operation:
    a) Use the timestamp when this merge was done.
    b) Use the latest timestamp between the two timestamps corresponding to the two databases under consideration.
    Here, we have used option b. 
    """
    db1_logs = db_logs_map[db1]
    db2_logs = db_logs_map[db2]

    for pk in primary_keys:
        if(db2_logs[pk][0] > db1_logs[pk][0]):
            print(f"Merge: {db1}({db1_logs[pk][0]}) < {db2}({db2_logs[pk][0]})")
            db1_oplogs = open(f"oplogs.{db1.lower()}", "a")
            db1_oplogs.write(f"{db2_logs[pk][0]}, {db1}.SET(({pk[0],pk[1]}), {db2_logs[pk][1]})")
            db1_oplogs.close()   
            db_set(db_name=db1, pk=pk, value=db2_logs[pk][1], ts=db2_logs[pk][0])

def db_get(db_name:str, pk:tuple)->str:
    if pk not in primary_keys:
        print(f"{pk} not in primary keys.")
        return
    if(db_name=="POSTGRESQL"):
        return postgre_handler.get("student_course_grades", pk)
    elif(db_name=="MONGODB"):
        return mongo_handler.get("university_db", "grades_of_students", pk)
    else: 
        print("HIVE")
        print(pk)
        return hive_handler.select_data(pk)
    #return db_logs_map[db_name][pk]

def parse_testcase_file(file_path):
    mongo_logger = open('oplogs.mongodb', 'w')
    hive_logger = open('oplogs.hive', 'w')
    postgresql_logger = open('oplogs.postgresql', 'w')

    with open(file_path, 'r') as file:
        for idx, line in enumerate(file):
            line = line.strip()
            if not line:
                continue  # Skip empty lines

            # Row number becomes the timestamp
            timestamp = idx + 1
            db_timestamp = None
            operation = None
            db1 = db2 = student_id = course_id = grade = None

            # Check for leading timestamp (db_timestamp)
            if ',' in line:
                parts = line.split(',', 1)
                if parts[0].strip().isdigit():
                    db_timestamp = int(parts[0].strip())
                    line = parts[1].strip()

            # Parse MERGE
            if 'MERGE' in line:
                match = re.match(r'(\w+)\.MERGE\((\w+)\)', line)
                if match:
                    db1, db2 = match.groups()
                    operation = 'MERGE'

            # Parse SET
            elif 'SET' in line:
                match = re.match(r'(\w+)\.SET\(\(([^,]+),([^)]+)\),\s*([^)]+)\)', line)
                if match:
                    db1, student_id, course_id, grade = match.groups()
                    operation = 'SET'

            # Parse GET
            elif 'GET' in line:
                match = re.match(r'(\w+)\.GET\(([^,]+),([^)]+)\)', line)
                if match:
                    db1, student_id, course_id = match.groups()
                    operation = 'GET'

            if db1 == "MONGODB":
                if operation == "SET":
                    print(f"{db_timestamp}, {db1}.SET(({student_id},{course_id}), {grade})\n")
                    db_set(db_name=db1, pk=(student_id, course_id), value=grade, ts=timestamp)
                    mongo_logger.write(f"{timestamp}, {db1}.SET(({student_id},{course_id}), {grade})\n")
                elif operation == 'GET':
                    print(f"{db_timestamp}, {db1}.GET({student_id},{course_id}) = {db_get(db_name=db1, pk=(student_id, course_id))}\n")
                    mongo_logger.write(f"{timestamp}, {db1}.GET({student_id},{course_id})\n")
            
            elif db1 == "HIVE":
                if operation == "SET":
                    print(f"{db_timestamp}, {db1}.SET(({student_id},{course_id}), {grade})\n")
                    db_set(db_name=db1, pk=(student_id, course_id), value=grade, ts=timestamp)
                    hive_logger.write(f"{timestamp}, {db1}.SET(({student_id},{course_id}), {grade})\n")
                elif operation == 'GET':
                    print(f"{db_timestamp}, {db1}.GET({student_id},{course_id}) = {db_get(db_name=db1, pk=(student_id, course_id))}\n")
                    hive_logger.write(f"{timestamp}, {db1}.GET({student_id},{course_id})\n")

            elif db1 == "POSTGRESQL":
                if operation == "SET":
                    print(f"{db_timestamp}, {db1}.SET(({student_id},{course_id}), {grade})\n")
                    db_set(db_name=db1, pk=(student_id, course_id), value=grade, ts=timestamp)
                    postgresql_logger.write(f"{timestamp}, {db1}.SET(({student_id},{course_id}), {grade})\n")
                elif operation == 'GET':
                    print(f"{db_timestamp}, {db1}.GET({student_id},{course_id}) = {db_get(db_name=db1, pk=(student_id, course_id))}\n")
                    postgresql_logger.write(f"{timestamp}, {db1}.GET({student_id},{course_id})\n")

            if operation == "MERGE":
                print(f"{db1}.MERGE({db2})\n")
                read_oplogs(db=db1)
                read_oplogs(db=db2)
                merge(db1=db1, db2=db2, ts=timestamp)

    mongo_logger.close()
    postgresql_logger.close()
    hive_logger.close()

parse_testcase_file(file_path="example_testcase_3.in")