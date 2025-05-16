def db_set(db_name: str, pk: tuple, value: str, ts: int, mongo_handler=None, hive_handler=None, postgre_handler=None, db_logs_map=None, primary_keys=None):
    """
    Generic db_set dispatcher function.
    """

    if primary_keys and pk not in primary_keys:
        print(f"{pk} not in primary keys.")
        primary_keys.append(pk)
        

    if db_name == "POSTGRESQL":
        postgre_handler.set("student_course_grades", pk, value, ts)
    elif db_name == "MONGODB":
        mongo_handler.set("university_db", "grades_of_students", pk, value, ts)
    elif db_name == "HIVE":
        hive_handler.update_data(pk, value)

    if db_logs_map is not None:
        db_logs_map[db_name][pk] = (ts, value)
