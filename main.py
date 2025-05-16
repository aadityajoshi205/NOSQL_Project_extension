import csv
from collections import defaultdict
import json
from MongoDB_connect import MongoDBHandler
from postgresql_connector import PostgreSQLHandler
from hive import Hive
from parse_testcase import parse_testcase_file


# mongo_handler = MongoDBHandler()

primary_keys = []

# Initialize 2D dictionaries using defaultdict
mongo_logs = defaultdict(lambda: (0, ""))
hive_logs = defaultdict(lambda: (0, ""))
postgresql_logs = defaultdict(lambda: (0, ""))


# Path to your CSV file
csv_file_path = 'student_course_grades.csv'  # Replace with your actual path


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
postgre_handler = PostgreSQLHandler(
    host="localhost",
    port=5432,
    database="doshte",
    user="nande",
    password="050309",
    primary_keys=primary_keys,
    db_logs_map=db_logs_map
)

mongo_handler = MongoDBHandler(primary_keys=primary_keys)

hive_handler = Hive("student_grades", "localhost", 10000, "vaibhav", "CUSTOM", "Badminton@2468", primary_keys=primary_keys)
file_path = input("Enter the path of the testcase file: ").strip()
parse_testcase_file(
    file_path=file_path,
    mongo_handler=mongo_handler,
    hive_handler=hive_handler,
    postgre_handler=postgre_handler,
    db_logs_map=db_logs_map,
    primary_keys=primary_keys)