
def sync(db_handler,db_cache):
    for key, value in db_cache.items():
        pk= key
        timestamp, grade = value
        db_handler.set("university_db", "student_course_grades", pk, grade, timestamp)
        
    

    
