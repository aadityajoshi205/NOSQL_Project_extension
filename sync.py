
def sync(db_handler,db_cache):
    for key, value in db_cache.items():
        pk= key
        timestamp, grade, delete_flag = value
        if delete_flag:
            db_handler.delete("university_db", "student_course_grades", pk)
        else:
            db_handler.set("university_db", "student_course_grades", pk, grade, timestamp)
        
    

    
