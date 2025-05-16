

from MongoDB_connect import MongoDBHandler

mongo_handler=MongoDBHandler()

def db_set(db_name: str, pk: tuple, value: str, ts: int):
    if db_name == "MONGODB":
        mongo_handler.set("university_db", "grades_of_students", pk, value, ts)
    else:
        db_logs_map[db_name][pk] = (ts, value)
db_set(db_name="MONGODB", pk=("SID1033", "CSE016"), value="B", ts=1)