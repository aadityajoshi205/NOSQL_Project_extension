import csv
from collections import defaultdict
import json
from MongoDB_connect import MONGODBHANDLER
from postgresql_connector import POSTGRESQLHANDLER
from parse_testcase import parse_testcase_file
from parse_testcase_multithreading import parse_testcase_file_multithreading


# mongo_handler = MongoDBHandler()

Databases=["MONGODB", "POSTGRESQL"]
primary_keys = []

# Initialize 2D dictionaries using defaultdict

for db in Databases:
    globals()[db+"_logs"] = defaultdict(lambda: (0, ""))


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
        for db in Databases:
            globals()[db+"_logs"][key] = (0, grade)

db_logs_map = {}
for db in Databases:
    db_logs_map[db] = globals()[db+"_logs"]

db_handlers=[]

for db in Databases:
    globals()[db.lower()+"_handler"]=globals()[db+"HANDLER"](primary_keys=primary_keys)
    db_handlers.append(globals()[db.lower()+"_handler"])
file_path = input("Enter the path of the testcase file: ").strip()
# parse_testcase_file(
#     file_path=file_path,
#     db_handlers=db_handlers,
#     db_logs_map=db_logs_map,
#     primary_keys=primary_keys,
#     Databases=Databases
# )

parse_testcase_file_multithreading(
    file_path=file_path,
    db_handlers=db_handlers,
    db_logs_map=db_logs_map,
    primary_keys=primary_keys,
    Databases=Databases
)