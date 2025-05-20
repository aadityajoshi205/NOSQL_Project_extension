

from MongoDB_connect import MongoDBHandler



students_csv = 'student_course_grades.csv' 
mongo_handler = MongoDBHandler()

mongo_handler.bulk_insert_students_from_csv("university_db", "student_course_grades", students_csv)