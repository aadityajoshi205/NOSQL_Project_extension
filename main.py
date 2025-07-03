import csv
from collections import defaultdict
import json
from MongoDB_connect import MONGODBHANDLER
from postgresql_connector import POSTGRESQLHANDLER
from hive import HIVEHANDLER
from parse_testcase import parse_testcase_file
from parse_testcase_multithreading import parse_testcase_file_multithreading
from hive import HIVEHANDLER


Databases=["MONGODB", "POSTGRESQL"]

db_handlers=[]

for db in Databases:
    globals()[db.lower()+"_handler"]=globals()[db+"HANDLER"]()
    db_handlers.append(globals()[db.lower()+"_handler"])
file_path = input("Enter the path of the testcase file: ").strip()
parse_testcase_file(
    file_path=file_path,
    db_handlers=db_handlers,
    Databases=Databases
)
