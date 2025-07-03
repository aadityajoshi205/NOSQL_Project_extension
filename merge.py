def merge(db_cache_1,db_cache_2,db1):
    temp=[]
    for key in db_cache_2.keys():
        if key not in db_cache_1.keys():
            db_cache_1[key] = db_cache_2[key]
            logger=open('oplogs.' + db1.lower(), 'a')
            latest_ts, latest_value, delete_flag = db_cache_2[key]
            pk = key
            if delete_flag:
                logger.write(f"{latest_ts}, {db1}.DELETE(({pk[0]},{pk[1]}))\n")
            else:
                logger.write(f"{latest_ts}, {db1}.SET(({pk[0]},{pk[1]}), {latest_value})\n")
        else:
            if db_cache_2[key][0] > db_cache_1[key][0]:
                print("raplacing values")
                temp.append((db1,db_cache_1[key][0],key[0],key[1],db_cache_1[key][1],db_cache_1[key][2]))
                db_cache_1[key] = db_cache_2[key]
                logger=open('oplogs.' + db1.lower(), 'a')
                latest_ts, latest_value, delete_flag = db_cache_2[key]      #latest_ts,latest_value,delete_flag = db_cache_2[key]
                pk = key
                if delete_flag:
                    logger.write(f"{latest_ts}, {db1}.DELETE(({pk[0]},{pk[1]}))\n")
                else:
                    logger.write(f"{latest_ts}, {db1}.SET(({pk[0]},{pk[1]}), {latest_value})\n")
    return (db_cache_1,temp)