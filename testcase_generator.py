import csv
import random

def read_csv_data(csv_file):
    data = []
    with open(csv_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        print("Detected headers:", reader.fieldnames)
        for row in reader:
            sid = row.get('student-ID')
            cid = row.get('course-id')
            grade = row.get('grade')
            if sid and cid and grade:
                data.append((sid.strip(), cid.strip(), grade.strip()))
    return data

def generate_testcase_from_csv(csv_file="student_course_grades.csv", out_file="example_testcase2.in", num_lines=20):
    databases = ["HIVE", "POSTGRESQL", "MONGODB"]
    thread_counters = {db: 1 for db in databases}  # Separate counter for each DB
    ops = []

    csv_data = read_csv_data(csv_file)
    if not csv_data:
        print("No valid entries found in the CSV file.")
        return

    for _ in range(num_lines):
        op_type = random.choice(["SET", "GET", "MERGE"])
        db = random.choice(databases)

        if op_type == "MERGE":
            other_db = random.choice([d for d in databases if d != db])
            ops.append(f"{db}.MERGE({other_db})")
        else:
            sid, cid, grade = random.choice(csv_data)
            thread = thread_counters[db]
            thread_counters[db] += 1

            if op_type == "SET":
                ops.append(f"{thread}, {db}.SET(({sid},{cid}), {grade})")
            else:  # GET
                ops.append(f"{thread}, {db}.GET({sid},{cid})")

    with open(out_file, "w") as f:
        for op in ops:
            f.write(op + "\n")

    print(f"Testcase written to {out_file}")

# Run it
generate_testcase_from_csv("student_course_grades.csv", "example_testcase_2.in", num_lines=20)