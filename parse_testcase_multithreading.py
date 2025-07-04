import re
import time
from datetime import datetime, timezone
from sync import sync
from merge import merge
import concurrent.futures

def get_precise_timestamp():
    ns = time.time_ns()
    seconds = ns // 1_000_000_000
    nanoseconds = ns % 1_000_000_000
    dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S") + f".{nanoseconds:09d}Z"

def parse_testcase_file_multithreading(file_path, db_handlers, db_logs_map, primary_keys, Databases):
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=20)
    sync_futures = []


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

            handler=None
            for i in range(0, len(db_handlers)):
                if db1 == Databases[i]:
                    handler = db_handlers[i]

            if operation == "SET":
                timestamp=get_precise_timestamp()
                print(f"{timestamp}, {db1}.SET(({student_id},{course_id}), {grade})")
                globals()[db1 + "_cache"][(student_id, course_id)] = [timestamp, grade]
                logger=open('oplogs.' + db1.lower(), 'a')
                logger.write(f"{timestamp}, {db1}.SET(({student_id},{course_id}), {grade})\n")
                logger.close()

            # if operation == "SET":
            #     timestamp=datetime.now()
            #     print(f"{timestamp}, {db1}.SET(({student_id},{course_id}), {grade})")
            #     db_set(db_name=db1, pk=(student_id, course_id), value=grade, ts=timestamp,
            #            mongo_handler=db_handlers[0], postgre_handler=db_handlers[1],
            #            db_logs_map=db_logs_map, primary_keys=primary_keys)
            #     if db1 == "MONGODB":
            #         mongo_logger = open('oplogs.mongodb', 'a')
            #         mongo_logger.write(f"{timestamp}, {db1}.SET(({student_id},{course_id}), {grade})\n")
            #         mongo_logger.close()
            #     elif db1 == "POSTGRESQL":
            #         postgresql_logger = open('oplogs.postgresql', 'a')
            #         postgresql_logger.write(f"{timestamp}, {db1}.SET(({student_id},{course_id}), {grade})\n")
            #         postgresql_logger.close()

            if operation =="GET":
                temp=1
                if (student_id, course_id) in globals()[db1 + "_cache"].keys():
                    value = globals()[db1 + "_cache"][(student_id, course_id)][1]
                elif db1 in Databases:
                    value=handler.get("university_db", "student_course_grades", pk=(student_id, course_id))
                else:
                    value = None
                    print(f"Unknown DB for GET operation: {db1}")
                    temp=0
                if temp:
                    timestamp = get_precise_timestamp()
                    print(f"{timestamp}, {db1}.GET(({student_id},{course_id})) = {value}")
                    logger=open('oplogs.' + db1.lower(), 'a')
                    logger.write(f"{timestamp}, {db1}.GET(({student_id},{course_id})) = {value}\n")
                    logger.close()

            # if operation == "GET":
            #     if handler:
            #         if db1 == "MONGODB":
            #             value = handler.get("university_db", "grades_of_students", pk=(student_id, course_id))
            #         elif db1 == "POSTGRESQL":
            #             value = handler.get("student_course_grades", pk=(student_id, course_id))
            #         else:
            #             value = None
            #             print(f"Unknown DB for GET operation: {db1}")
            #         timestamp=datetime.now()

            #         print(f"{timestamp}, {db1}.GET({student_id},{course_id}) = {value}")
                    
            #         if db1 == "MONGODB":
            #             mongo_logger = open('oplogs.mongodb', 'a')
            #             mongo_logger.write(f"{timestamp}, {db1}.GET(({student_id},{course_id}))\n")
            #             mongo_logger.close()
            #         elif db1 == "POSTGRESQL":
            #             postgresql_logger = open('oplogs.postgresql', 'a')
            #             postgresql_logger.write(f"{timestamp}, {db1}.GET(({student_id},{course_id}))\n")
            #             postgresql_logger.close()
                    
            if operation=="MERGE":
                globals()[db1 + "_cache"]=merge(globals()[db1 + "_cache"], globals()[db2 + "_cache"],db1)
                # snapshot = dict(globals()[db1 + "_cache"])
                # future = executor.submit(sync, handler, snapshot)
                # sync_futures.append(future)

            # elif operation == "MERGE":
            #     if handler:
            #         print(f"{db1}.MERGE({db2})")
            #         handler.merge(db2)

    # executor.shutdown(wait=True)
    i=0
    for db in Databases:
        # sync(db_handlers[i], globals()[db + "_cache"])
        future = executor.submit(sync, db_handlers[i], globals()[db + "_cache"])
        sync_futures.append(future)
        i+=1
        # print(globals()[db + "_cache"])
    
    executor.shutdown(wait=True, timeout=120.0)
    for future in sync_futures:
        try:
            print(f"Future Result: {future.result}")  # will raise if the sync task failed
        except Exception as e:
            print("Sync failed:", e)
