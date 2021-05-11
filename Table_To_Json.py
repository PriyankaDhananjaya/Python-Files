import json, pyodbc, collections
from datetime import datetime
from joblib import Parallel, delayed
from decimal import Decimal
from utilitiesClass import utilities

with open('values.json') as v:
    variables = json.load(v)

    jsonFilePath = variables['tblVolume_JsonFilePath']   
    source_ConnString = variables['0107-02-0774-01_NGDB_ConnectionString']   
tableName = variables['TableName_tblVolume']  
table_JsonArray = []
TotalRows = 0
RowsProcessed = 0

class CustomJsonEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(CustomJsonEncoder, self).default(obj)

def processRow(row):
    global table_JsonArray, RowsProcessed
    RowsProcessed = RowsProcessed + 1
    print('     '+str(RowsProcessed) + ' / ' + str(TotalRows) + ' rows processed', end='\r')
    d = collections.OrderedDict()
    d["VolumeId"] = row[0]
    d["VolumeDay"] = row[1]
    d["VolumeTypeId"]= row[2]
    d["MemberId"] = row[3]
    d["PositiveAmount"] = Decimal(str(row[4]))
    d["NegativeAmount"] = Decimal(str(row[5]))
    d["RegionId"] = row[6]
    table_JsonArray.append(d)
print()

def read(conn):

        startVolumeDay = 0
        endVolumeDay = -1
        rowId,startVolumeDay,endVolumeDay,tryCount = utilities.getVolumeDaysToArchive()
        varVolumeDay = int(0)

        global table_JsonArray

        endTime = datetime.now()
        startTime = datetime.now()

        try :

            utilities.updateDataArchiveLogs(rowId,None,None,int(1),None,None,None, None, None)
            cursor=conn.cursor()
            
            params = (startVolumeDay,endVolumeDay)
            
            startTime = datetime.now()
            
            print('Processing Volume Days : ' + str(startVolumeDay) + ' TO ' + str(endVolumeDay))
            print('Fetching Data from DB for Volume Days : ' + str(startVolumeDay) + ' TO ' + str(endVolumeDay))
           
            cursor.execute('{CALL NGDB.dataArchive.getVolumeDataRange2 (?,?)}',params)
           
            print('Sucessfully Retrieved Data from DB for Volume Days : ' + str(startVolumeDay) + ' TO ' + str(endVolumeDay))
            
            rows = cursor.fetchall()
        
            if(len(rows) > 0):
                
                utilities.updateDataArchiveLogs(rowId,startTime,None,int(2),None,None, None, None, None)
                global TotalRows
                TotalRows = len(rows)

                Parallel(n_jobs=100, backend="threading")(delayed(processRow)(row) for row in rows)
                
                j = json.dumps(table_JsonArray,separators=(',', ':'), cls=CustomJsonEncoder)
                table_JsonArray = []

                endTime =  datetime.now()
                #Get File Name in format to store from Utilities Class
                jsonFilePath = utilities.getFilePath(tableName,startVolumeDay,endVolumeDay)

                with open(jsonFilePath, "w") as f:
                    f.write(j)

                print('sucessfully created '+ jsonFilePath +' json file for ' + str(startVolumeDay) + ' TO ' + str(endVolumeDay))
                print("Time taken to process :" + str(endTime - startTime))
                
                utilities.updateDataArchiveLogs(rowId,startTime,endTime,int(3),int(TotalRows),None, int(tryCount)+1, None, jsonFilePath)
                
                #utilities.validateJsonFileData(jsonFilePath,rowId,int(TotalRows),startVolumeDay,endVolumeDay)

            else:
                print('There is no record found for the date range ' + str(startVolumeDay) + ' TO ' + str(endVolumeDay))

        except Exception as e :
            print(e)         
            print('There was an error creating '+ tableName +' json file !')
            
            utilities.updateDataArchiveLogs(rowId,None,None,int(5),None,None, None, None, None)
            
        conn.close()
        if(startVolumeDay == 0):
            print('No Volume Days Found to Process from Log Table !')

conn = pyodbc.connect(source_ConnString)
read(conn)


