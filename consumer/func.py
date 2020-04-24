import datetime
import cx_Oracle

def makeDictFactory(cursor):
    columnNames = [d[0] for d in cursor.description]

    def createRow(*args):
        return dict(zip(columnNames, args))

    return createRow

def getSerial(result):
    _list = list(result.keys())
    for item in _list:
        if isinstance(result[item], datetime.datetime):
           result[item] = result.pop(item).strftime("%d-%b-%Y (%H:%M:%S.%f)")
        if isinstance(result[item], cx_Oracle.LOB):
           result[item] = cx_Oracle.LOB.read(result[item])
    return result


def getOrigin(result):
    _list = list(result.keys())
    for item in _list:
        if "DATE" in item:
           try:
               result[item]=datetime.datetime.strptime(result[item], "%d-%b-%Y (%H:%M:%S.%f)")
           except:
                  return result
    return result
