import re
from MongoDB_connect import MongoDBHandler
from hive import Hive
from postgresql_connector import PostgreSQLHandler
from db_set import db_set
from db_get import db_get
from datetime import datetime

def parse_testcase_file(file_path, mongo_handler, hive_handler, postgre_handler, db_logs_map, primary_keys):
    system_handlers = {
        "MONGODB": mongo_handler,
        "HIVE": hive_handler,
        "POSTGRESQL": postgre_handler
    }
    for oplog_file in ['oplogs.mongodb', 'oplogs.postgresql', 'oplogs.hive']:
        open(oplog_file, 'w').close()

    with open(file_path, 'r') as file:
        for idx, line in enumerate(file):
            line = line.strip()
            if not line:
                continue


            db_timestamp = None
            operation = None
            db1 = db2 = student_id = course_id = grade = None

            if ',' in line:
                parts = line.split(',', 1)
                if parts[0].strip().isdigit():
                    db_timestamp = int(parts[0].strip())
                    line = parts[1].strip()

            if 'MERGE' in line:
                match = re.match(r'(\w+)\.MERGE\((\w+)\)', line)
                if match:
                    db1, db2 = match.groups()
                    operation = 'MERGE'

            elif 'SET' in line:
                match = re.match(r'(\w+)\.SET\(\(([^,]+),([^)]+)\),\s*([^)]+)\)', line)
                if match:
                    db1, student_id, course_id, grade = match.groups()
                    operation = 'SET'

            elif 'GET' in line:
                match = re.match(r'(\w+)\.GET\(([^,]+),([^)]+)\)', line)
                if match:
                    db1, student_id, course_id = match.groups()
                    operation = 'GET'
            elif 'FULL_SYNC' in line:
                    # --- FULL SYNC block ---
                    print("[INFO] Full Sync started between all databases...")

                    mongo_handler.merge('HIVE')
                    mongo_handler.merge('POSTGRESQL')
                    hive_handler.merge('MONGODB')
                    postgre_handler.merge('MONGODB')

                    print("[INFO] Full Sync completed.")

            handler = system_handlers.get(db1)

            if operation == "SET":
                timestamp=datetime.now()
                print(f"{timestamp}, {db1}.SET(({student_id},{course_id}), {grade})")
                db_set(db_name=db1, pk=(student_id, course_id), value=grade, ts=timestamp,
                       mongo_handler=mongo_handler, hive_handler=hive_handler, postgre_handler=postgre_handler,
                       db_logs_map=db_logs_map, primary_keys=primary_keys)
                if db1 == "MONGODB":
                    mongo_logger = open('oplogs.mongodb', 'a')
                    mongo_logger.write(f"{timestamp}, {db1}.SET(({student_id},{course_id}), {grade})\n")
                    mongo_logger.close()
                elif db1 == "POSTGRESQL":
                    postgresql_logger = open('oplogs.postgresql', 'a')
                    postgresql_logger.write(f"{timestamp}, {db1}.SET(({student_id},{course_id}), {grade})\n")
                    postgresql_logger.close()
                elif db1 == "HIVE":
                    hive_logger = open('oplogs.hive', 'a')
                    hive_logger.write(f"{timestamp}, {db1}.SET(({student_id},{course_id}), {grade})\n")
                    hive_logger.close()

            elif operation == "GET":
                if handler:
                    if db1 == "MONGODB":
                        value = handler.get("university_db", "grades_of_students", pk=(student_id, course_id))
                    elif db1 == "POSTGRESQL":
                        value = handler.get("student_course_grades", pk=(student_id, course_id))
                    elif db1 == "HIVE":
                        value = handler.select_data(pk=(student_id, course_id))
                    else:
                        value = None
                        print(f"Unknown DB for GET operation: {db1}")
                    timestamp=datetime.now()

                    print(f"{timestamp}, {db1}.GET({student_id},{course_id}) = {value}")
                    
                    if db1 == "MONGODB":
                        mongo_logger = open('oplogs.mongodb', 'a')
                        mongo_logger.write(f"{timestamp}, {db1}.GET(({student_id},{course_id}))\n")
                        mongo_logger.close()
                    elif db1 == "POSTGRESQL":
                        postgresql_logger = open('oplogs.postgresql', 'a')
                        postgresql_logger.write(f"{timestamp}, {db1}.GET(({student_id},{course_id}))\n")
                        postgresql_logger.close()
                    elif db1 == "HIVE":
                        hive_logger = open('oplogs.hive', 'a')   
                        hive_logger.write(f"{timestamp}, {db1}.GET(({student_id},{course_id}))\n")
                        hive_logger.close()
                    


            elif operation == "MERGE":
                if handler:
                    print(f"{db1}.MERGE({db2})")
                    handler.merge(db2)
