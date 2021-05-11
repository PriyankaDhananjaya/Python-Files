import json, pyodbc
from datetime import datetime
from os import listdir

with open('values.json') as v:
    variables = json.load(v)
source_ConnString = variables['0107-02-0774-01_DataArchive_ConnectionString'] 
jsonFilePath = variables['tblVolume_JsonFilePath']   

class utilities:

  def getFilePath(tableName,startVolumeDay,endVolumeDay):
    filePathToSave = jsonFilePath + tableName + "_" +str(startVolumeDay) + "_"+ str(endVolumeDay) + "_" + datetime.now().strftime('%Y%m%d%H%M%S%f') + '.Json'
    return filePathToSave 


  def getVolumeDaysToArchive():
    conn = pyodbc.connect(source_ConnString)
    cursor=conn.cursor()
    cursor.execute('{CALL uspGetDataToArchive}')
    rows = cursor.fetchall()
    
    if( len(rows) == 0 ):
      return 0, 0, -1, 0
    else:
      for row in rows:
        rowId = row[0]
        sDate = row[1]
        eDate = row[2]
        tryCount = row[3]
        return int(rowId), int(sDate), int(eDate), int(tryCount)
    conn.close()

  def updateDataArchiveLogs(rowId, executionStartDate, executionEndDate, Status, ExecutedRows, RowsInJson, TryCount, IsJsonChecked, JsonFileName):
      conn = pyodbc.connect(source_ConnString)
      print(rowId)
      cursor = conn.cursor()
      params = (rowId,executionStartDate,executionEndDate,Status, TryCount, ExecutedRows, JsonFileName, RowsInJson, IsJsonChecked)
      cursor.execute('{CALL uspUpdateDataArchiveLogs ( ?, ?, ?, ?, ?, ?, ?, ?, ? )}',params)
      cursor.commit()
      conn.close()


     

  def validateJsonFileData(jsonFilePath, rowId, noOfRowsProcessed, startDay, endDay):
      try:
        with open(jsonFilePath) as f:
          data = json.load(f)
        rowsInJsonFile = len(data)
        
        conn = pyodbc.connect(source_ConnString)
        cursor=conn.cursor()
        params = (startDay,endDay)
        cursor.execute('{CALL NGDB.dataArchive.getVolumeDataRange2 (?,?)}',params)
        rows = cursor.fetchall()
        noOfRowsInDB = len(rows)

        if( rowsInJsonFile == noOfRowsInDB == noOfRowsProcessed):
          utilities.updateDataArchiveLogs(rowId,startDay,endDay,int(4),None,rowsInJsonFile,None, True, None)
          print('Matches')

      finally:
        f.close()
        conn.close()
