import re
from datetime import datetime

def read_oplogs(db: str) -> dict:
    logs = {}
    datetime_format = "%Y-%m-%d %H:%M:%S.%f"
    with open(f"oplogs.{db.lower()}", 'r') as file:
        # print(f"Reading oplogs for {db}")
        for idx, line in enumerate(file):
            line = line.strip()
            if not line:
                continue  # Skip empty lines

            db_timestamp = None
            operation = None
            student_id = course_id = grade = None

            # Check for leading timestamp (db_timestamp)
            if ',' in line:
                    parts = line.split(',', 1)
                    try:
                        db_timestamp = datetime.strptime(parts[0].strip(), datetime_format)
                        line = parts[1].strip()
                    except ValueError:
                        # Parsing failed — handle it if needed
                        pass

            # Parse SET operation
            if 'SET' in line:
                match = re.match(r'(\w+)\.SET\(\(\s*([^,]+)\s*,\s*([^)]+)\s*\),\s*(.+)\)', line)
                if match:
                    db_name, student_id, course_id, grade = match.groups()
                    operation = 'SET'

            # Parse GET operation
            elif 'GET' in line:
                match = re.match(r'(\w+)\.GET\(\s*([^,]+)\s*,\s*([^)]+)\s*\)', line)
                if match:
                    db_name, student_id, course_id = match.groups()
                    operation = 'GET'

            # print(f"Operation = {operation}")

            # Handle SET
            if operation == 'SET':
                # print(f"SET: {db_name} ({db_timestamp}) {student_id}, {course_id} => {grade}")
                # Only store if db matches (optional check)
                if db_name.upper() == db.upper():
                    key = (student_id.strip(), course_id.strip())
                    logs[key] = (db_timestamp, grade.strip())

            # Handle GET if needed (currently you don't do anything for GET)
            elif operation == 'GET':
                # print(f"GET: {db_name} ({db_timestamp}) {student_id}, {course_id}")
                # Usually GET is a read, so you may not want to modify `logs`
                pass
    return logs
