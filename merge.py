from datetime import datetime

def merge(db_cache_1,db_cache_2):
    for key in db_cache_2.keys():
        if key not in db_cache_1.keys():
            db_cache_1[key] = db_cache_2[key]
        else:
            if db_cache_2[key][0] > db_cache_1[key][0]:
                print("raplacing values")
                db_cache_1[key] = db_cache_2[key]
    return db_cache_1