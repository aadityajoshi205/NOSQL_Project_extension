import re
import time
from datetime import datetime, timezone
from sync import sync
from merge import merge

undo=[]

def get_precise_timestamp():
    ns = time.time_ns()
    seconds = ns // 1_000_000_000
    nanoseconds = ns % 1_000_000_000
    dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S") + f".{nanoseconds:09d}Z"

def parse_testcase_file(file_path, db_handlers,Databases):

    for db in Databases:
        globals()[db + "_cache"]= {} 


    for oplog_file in ["oplogs." + db.lower() for db in Databases]:
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
            elif 'DELETE' in line:
                match = re.match(r'(\w+)\.DELETE\(([^,]+),([^)]+)\)', line)
                if match:
                    db1, student_id, course_id = match.groups()
                    operation = 'DELETE'
            elif 'FULL_SYNC' in line:
                    # --- FULL SYNC block ---
                    print("[INFO] Full Sync started between all databases...")
                    for i in range(1, len(db_handlers)):
                        db_cache_1 = globals()[Databases[0] + "_cache"]
                        db_cache_2= globals()[Databases[i] + "_cache"]
                        globals()[Databases[0] + "_cache"]=merge(db_cache_1,db_cache_2,Databases[0])
                    for i in range(1, len(db_handlers)):
                        db_cache_1 = globals()[Databases[i] + "_cache"]
                        db_cache_2= globals()[Databases[0] + "_cache"]
                        globals()[Databases[i] + "_cache"]=merge(db_cache_1,db_cache_2,Databases[i])

                    # db_handlers[0].merge('POSTGRESQL')
                    # db_handlers[1].merge('MONGODB')

                    print("[INFO] Full Sync completed.")
            
            elif 'COMMIT' in line:
                i=0
                for db in Databases:
                    sync(db_handlers[i], globals()[db + "_cache"])
                    i+=1
                    globals()[db+"_cache"].clear()
            
            elif 'ROLLBACK' in line:
                for db in Databases:
                    globals()[db+"_cache"].clear()
                
            elif 'UNDO' in line:
                operation='UNDO'

            handler=None
            for i in range(0, len(db_handlers)):
                if db1 == Databases[i]:
                    handler = db_handlers[i]

            if operation == "SET":
                timestamp=get_precise_timestamp()
                print(f"{timestamp}, {db1}.SET(({student_id},{course_id}), {grade})")
                if (student_id,course_id) in globals()[db1+"_cache"].keys():
                    undo.append([(db1,globals()[db1+"_cache"][(student_id,course_id)][0],student_id,course_id,globals()[db1+"_cache"][(student_id,course_id)][1],globals()[db1+"_cache"][(student_id,course_id)][2])])
                globals()[db1 + "_cache"][(student_id, course_id)] = [timestamp, grade, 0]
                logger=open('oplogs.' + db1.lower(), 'a')
                logger.write(f"{timestamp}, {db1}.SET(({student_id},{course_id}), {grade})\n")
                logger.close()


            if operation =="GET":
                temp=1
                if (student_id, course_id) in globals()[db1 + "_cache"].keys() and globals()[db1 + "_cache"][(student_id, course_id)][2] == 0:
                    value = globals()[db1 + "_cache"][(student_id, course_id)][1]
                elif db1 in Databases and not globals()[db1 + "_cache"][(student_id, course_id)][2] == 1:
                    value=handler.get("university_db", "student_course_grades", pk=(student_id, course_id))
                else:
                    value = None
                    print(f"({student_id}, {course_id}) not found in {db1}")
                    temp=0
                if temp:
                    timestamp = get_precise_timestamp()
                    print(f"{timestamp}, {db1}.GET(({student_id},{course_id})) = {value}")
                    logger=open('oplogs.' + db1.lower(), 'a')
                    logger.write(f"{timestamp}, {db1}.GET(({student_id},{course_id})) = {value}\n")
                    logger.close()
                    
            if operation=="MERGE":
                globals()[db1 + "_cache"],temp=merge(globals()[db1 + "_cache"], globals()[db2 + "_cache"],db1)
                undo.append(temp)

            if operation == "DELETE":
                timestamp=get_precise_timestamp()
                print(f"{timestamp}, {db1}.DELETE(({student_id},{course_id}), {grade})")
                if (student_id,course_id) in globals()[db1+"_cache"].keys():
                    undo.append([(db1,globals()[db1+"_cache"][(student_id,course_id)][0],student_id,course_id,globals()[db1+"_cache"][(student_id,course_id)][1],globals()[db1+"_cache"][(student_id,course_id)][2])])
                globals()[db1 + "_cache"][(student_id, course_id)] = [timestamp, grade, 1]
                logger=open('oplogs.' + db1.lower(), 'a')
                logger.write(f"{timestamp}, {db1}.DELETE(({student_id},{course_id}), {grade})\n")
                logger.close()
            
            if operation == "UNDO":
                temp=undo.pop()
                for i in temp:
                    db1,timestamp,student_id,course_id,value,delete_flag=i
                    globals()[db1 + "_cache"][(student_id, course_id)] = [timestamp, value, delete_flag]

    i=0
    for db in Databases:
        sync(db_handlers[i], globals()[db + "_cache"])
        i+=1
        print(globals()[db + "_cache"])