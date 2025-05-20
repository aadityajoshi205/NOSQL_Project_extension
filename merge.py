from datetime import datetime

def merge(db_cache_1,db_cache_2,db1):
    for key in db_cache_2.keys():
        if key not in db_cache_1.keys():
            db_cache_1[key] = db_cache_2[key]
            logger=open('oplogs.' + db1.lower(), 'a')
            latest_ts, latest_value = db_cache_2[key]
            pk = key
            logger.write(f"{latest_ts}, {db1}.SET(({pk[0]},{pk[1]}), {latest_value})\n")
        else:
            if db_cache_2[key][0] > db_cache_1[key][0]:
                print("raplacing values")
                db_cache_1[key] = db_cache_2[key]
                logger=open('oplogs.' + db1.lower(), 'a')
                latest_ts, latest_value = db_cache_2[key]
                pk = key
                logger.write(f"{latest_ts}, {db1}.SET(({pk[0]},{pk[1]}), {latest_value})\n")
    return db_cache_1