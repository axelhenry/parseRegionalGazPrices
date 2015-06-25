# -*- coding: utf-8 -*-


def getPricesSQL(aConn, aLogger, aId):
    dict = {}
    cursor = aConn.cursor()
    cursor.execute(
        'SELECT gaz_name, gaz_price FROM gazprices WHERE regional_id=%s;',
        (aId,))
    for elem in cursor:
        dict[elem[0]] = elem[1]
    return dict


def addPricesSQL(aConn, aLogger, aId, aDict):
    cursor = aConn.cursor()
    for key, value in aDict.items():
        tuple = (key, value, aId)
        aLogger.info(
            'DatabaseHandler :: addPricesSQL : inserting %s, %s, %s',
            tuple[0], tuple[1], tuple[2])
        cursor.execute(
            'INSERT INTO gazprices(gaz_name, gaz_price,regional_id) VALUES (%s, %s,%s);',
            tuple)
    cursor.close()


def deleteValuesSQL(aConn, aLogger, aId):
    cursor = aConn.cursor()
    cursor.execute('DELETE FROM gazprices WHERE regional_id=%s;', (aId,))
    result = int(cursor.statusmessage.replace('DELETE ', ''))
    return result


def checkForRegionSQL(aConn, aLogger, aRegion):
    cursor = aConn.cursor()
    cursor.execute(
        'SELECT id FROM regional_id WHERE regional_name=%s;', (aRegion,))
    result = cursor.fetchone()
    aLogger.info(
        'DatabaseHandler :: checkForRegionSQL : result => %s', result)
    cursor.close()
    if result:
        return result[0]
    else:
        return None


def addRegionToDbSQL(aConn, aLogger, aRegion):
    cursor = aConn.cursor()
    cursor.execute(
        'INSERT INTO regional_id (regional_name) VALUES (%s);', (aRegion,))
    cursor.execute("SELECT LASTVAL();")
    result = cursor.fetchone()
    aLogger.info(
        'DatabaseHandler :: checkForRegionSQL : result => %s', result)
    cursor.close()
    if result:
        return result[0]
    else:
        return None


def processFunctionOnDb(aPool, aLogger, func, *args):
    try:
        aLogger.info(
            'DatabaseHandler :: processed function : %s', func.__name__)
        with aPool.getconn() as myDb:
            myResult = func(myDb, aLogger, *args)
            return myResult
    except Exception as e:
        raise e
    finally:
        myDb.commit()
        myDb.close()
        aPool.putconn(myDb)
