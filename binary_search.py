from datetime import datetime

def binary_search_datetime(file_path, target_ts_str):
    target_ts = datetime.strptime(target_ts_str, "%Y-%m-%d %H:%M:%S.%f")

    with open(file_path, "r") as f:
        f.seek(0, 2)
        file_size = f.tell()
        low, high = 0, file_size

        while low < high:
            mid = (low + high) // 2
            f.seek(mid)

            if mid != 0:
                f.readline()

            line_start = f.tell()
            line = f.readline()

            if not line:
                break

            try:
                parts = line.strip().split(maxsplit=2)
                line_ts = datetime.strptime(parts[0] + " " + parts[1], "%Y-%m-%d %H:%M:%S.%f")
            except Exception:
                return None  # Invalid timestamp format

            if line_ts == target_ts:
                return line.strip()  # Found exact match #Line ko split karengey using REGEX - timestamp, primary_key, grade
            elif line_ts < target_ts:
                low = f.tell()
            else:
                high = line_start

    return None  # Not found