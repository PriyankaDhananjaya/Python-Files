import json
import pyodbc
import collections
import datetime

with open('values.json') as v:
    variables = json.load(v)

jsonFilePath = variables['jsonFolderPath']   
source_ConnString = variables['DevSql_Server'] 

class CustomJsonEncoder(json.JSONEncoder):

 def default(self, obj):
    if isinstance(obj, Decimal):
        return float(obj)
    return super(CustomJsonEncoder, self).default(obj)

def dateFormat(o):
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()

def read(conn):

    try :

        cursor=conn.cursor()
        cursor.execute('{CALL uspGetAutoshiptemplatesForMigration}')
        autoships = cursor.fetchall()
        tblAutoshipColumnNames = [column[0] for column in cursor.description]
        autoshipsProdSet = []
        autoshipsLineItems = []
        shippingAddress = []
        orderpayments = []

        if(cursor.nextset() == True):
            autoshipsProdSet = cursor.fetchall()
            tblAutoshipProdSetColumnNames = [column[0] for column in cursor.description]
            
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

        # Convert query to objects of key-value pairs
        table_JsonObject = {}
        tblProdSets_Obj = {}
        tblProdSetLineItems_Obj = {}
        tblShippingAddress_Obj = {}
        tblOrderPayments_Obj = {}
        autoshipArray = []

        for row in autoships:
            autoshipId = row[0]
            shippingAddressId = row[12]
            memberId = row[1]
            d= dict( zip( tblAutoshipColumnNames , row ) )
            productSetsArray = []
            lineItemsArray = []
            shippingAddressArray = []
            orderPaymentsArray = []
            orderPaymentAddressArray = []

            for ps in autoshipsProdSet:
                if ps[0] == autoshipId:
                    productSetsArray.append(dict( zip( tblAutoshipProdSetColumnNames , ps ) ) )
                   
            for li in autoshipsLineItems:
                if li[0] == autoshipId:
                    lineItemsArray.append(dict( zip( tblAutoshipProdSetLineItemColumnNames , li ) ) )
            
            for sa in shippingAddress:
                if sa[0] == shippingAddressId:
                    shippingAddressArray.append(dict( zip( shippingAddressColumnNames , sa ) ) )

            for op in orderpayments:
                orderPaymentAddressId = op[10]
                if op[2] == memberId:
                    orderPaymentsArray.append(dict( zip( tblOrderpaymentsColumnNames , op ) ) )
                    for oa in orderPaymentAddress:
                        if oa[0] == orderPaymentAddressId:
                            orderPaymentAddressArray.append(dict( zip( orderPaymentAddressColumnNames , oa ) ) )

            tblProdSets_Obj = productSetsArray
            tblProdSetLineItems_Obj = lineItemsArray
            tblShippingAddress_Obj = shippingAddressArray
            tblOrderPayments_Obj = orderPaymentsArray
            tblOrderPaymentAddress_Obj = orderPaymentAddressArray

            d["tblAutoshipProductSets"] = tblProdSets_Obj
            d["tblAutoshipProductSetLineItems"] = tblProdSetLineItems_Obj
            d["shippingAddress"] = tblShippingAddress_Obj
            d["tblOrderPayments"] = tblOrderPayments_Obj
            d["tblOrderPaymentAddress"] = tblOrderPaymentAddress_Obj

            autoshipArray.append(d)
            table_JsonObject["tblAutoships"] = autoshipArray
            j = json.dumps(table_JsonObject,indent=9, default= dateFormat)
            # j = json.dumps(table_JsonObject,separators=(',', ':'), cls=CustomJsonEncoder,default= dateFormat)


        with open("AutoshipTemplates.json", "w") as f:
            f.write(j)

        # print('sucessfully created '+ tableName +' json file !')

    except Exception as e :
         print(e)         
        #  print('There was an error creating '+ tableName +' json file !')
    finally:             
            conn.close()

conn = pyodbc.connect(source_ConnString)
read(conn)


