import json
import pyodbc
import collections
import datetime
from decimal import Decimal 
from joblib import Parallel, delayed

with open('values.json') as v:
    variables = json.load(v)

jsonFilePath = variables['jsonFolderPath']   
source_ConnString = variables['DevSql_Server'] 
TotalRows = 0
RowsProcessed = 0
ai =  variables['autoshipId_Index']
ami = variables['autoship_MemberId_Index']
asi = variables['autoship_ShippingAddressId_Index']
aai = variables['autoship_AddressId_Index']
omi = variables['orderPayments_MemberId_Index']
oai = variables['orderPayments_AddressId_Index']

class CustomJsonEncoder(json.JSONEncoder):

 def default(self, obj):
    if isinstance(obj, Decimal):
        return float(obj)
    return super(CustomJsonEncoder, self).default(obj)

def dateFormat(o):
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()

def processRow(row):
    
    tblProdSets_Obj = {}
    tblProdSetLineItems_Obj = {}
    tblShippingAddress_Obj = {}
    tblOrderPayments_Obj = {}
    global RowsProcessed,autoshipArray
    RowsProcessed = RowsProcessed + 1
    print('     '+str(RowsProcessed) + ' / ' + str(TotalRows) + ' rows processed', end='\r')
    global tblAutoshipColumnNames,autoshipsLineItems,shippingAddress,orderpayments
    global tblAutoshipProdSetColumnNames,tblAutoshipProdSetLineItemColumnNames,shippingAddressColumnNames
    global tblOrderpaymentsColumnNames,orderPaymentAddressColumnNames,orderPaymentAddress
    global autoshipArray
    autoshipArray = []

    autoshipId = row[ai]
    shippingAddressId = row[asi]
    memberId = row[ami]
    d= dict( zip( tblAutoshipColumnNames , row ) )
    print(d)
    lineItemsArray = []
    shippingAddressArray = []
    orderPaymentsArray = []
    orderPaymentAddressArray = []

    for li in list(filter(lambda x: x[ai] == autoshipId in x,autoshipsLineItems)):
        print("First Loop")
        ld = dict( zip( tblAutoshipProdSetLineItemColumnNames , li ) )
        ld.pop("AutoshipId")
        lineItemsArray.append(ld)
    
    for sa in  list(filter(lambda x: x[aai] == shippingAddressId in x,shippingAddress)):
        print("Second Loop")
        sd = dict( zip( shippingAddressColumnNames , sa ) )
        sd.pop("AddressId")
        shippingAddressArray.append(sd)

    for op in list(filter(lambda x: x[omi] == memberId in x,orderpayments)):
        print("Third Loop")
        orderPaymentAddressId = op[oai]
        od = dict( zip( tblOrderpaymentsColumnNames , op ) ) 
        od.pop("MemberId")
        orderPaymentsArray.append(od)
        for oa in list(filter(lambda x: x[aai] == orderPaymentAddressId in x,orderPaymentAddress)):
            print("Four Loop")
            pd = dict( zip( orderPaymentAddressColumnNames , oa ) )
            pd.pop("AddressId")
            orderPaymentAddressArray.append(pd)

    tblProdSetLineItems_Obj = lineItemsArray
    tblShippingAddress_Obj = shippingAddressArray
    tblOrderPayments_Obj = orderPaymentsArray
    tblOrderPaymentAddress_Obj = orderPaymentAddressArray

    d["AutoshipLineItems"] = tblProdSetLineItems_Obj
    d["ShippingAddress"] = tblShippingAddress_Obj
    d["OrderPayment"] = tblOrderPayments_Obj
    d["OrderPaymentAddress"] = tblOrderPaymentAddress_Obj
    autoshipArray.append(d)

    # print(autoshipArray)
    startTime = datetime.datetime.now()
    # Convert query to objects of key-value pairs
    table_JsonObject = {}

    table_JsonObject["Autoships"] = autoshipArray
    autoshipArray = []
    j = json.dumps(table_JsonObject,indent=9, default= dateFormat)
    #j = json.dumps(table_JsonObject,separators=(',', ':'), cls=CustomJsonEncoder,default= dateFormat)

    
    print(str(memberId)+ '_'+ str(autoshipId)  + "_Autoship_template.JSON")

    with open(str(memberId)+ '_Autoship_Template_' + startTime.strftime('%Y%m%d%H%M%S%f') + '.Json' , "w") as f:
        f.write(j)
        f.close()
        
    endTime = datetime.datetime.now()    
    input("Time taken to process :" + str(endTime - startTime))

    


def read(conn):

    try :

        params = int(input("Enter cohort number of the members to fetch Autoships: "))
        cursor=conn.cursor()
        cursor.execute('{CALL uspGetAutoshiptemplatesForMigration (?)}',params) 
        autoships = cursor.fetchall()

        global TotalRows
        TotalRows = len(autoships)
        global  tblAutoshipColumnNames
        tblAutoshipColumnNames = [column[0] for column in cursor.description]
        global  autoshipsLineItems 
        autoshipsLineItems = []
        global  shippingAddress
        shippingAddress = []
        global  orderpayments
        orderpayments = []
        global tblAutoshipProdSetColumnNames,tblAutoshipProdSetLineItemColumnNames,shippingAddressColumnNames
        global tblOrderpaymentsColumnNames,orderPaymentAddressColumnNames,orderPaymentAddress
        
            
        if(cursor.nextset() == True):
            autoshipsLineItems = cursor.fetchall()
            tblAutoshipProdSetLineItemColumnNames = [column[0] for column in cursor.description]
        
        if(cursor.nextset() == True):
            shippingAddress = cursor.fetchall()
            shippingAddressColumnNames = [column[0] for column in cursor.description]
        
        if(cursor.nextset() == True):
            orderpayments = cursor.fetchall()
            tblOrderpaymentsColumnNames = [column[0] for column in cursor.description]

        if(cursor.nextset() == True):
            orderPaymentAddress = cursor.fetchall()
            orderPaymentAddressColumnNames = [column[0] for column in cursor.description]
        

        # for row in autoships:
        #     processRow(row)
        Parallel(n_jobs=100, backend="threading")(delayed(processRow)(row) for row in autoships)

        
        # print('sucessfully created '+ tableName +' json file !')

    except Exception as e :
         print(e)         
        #  print('There was an error creating '+ tableName +' json file !')
    finally:             
            conn.close()

conn = pyodbc.connect(source_ConnString)
read(conn)


