#!/usr/bin/env python
# coding: utf-8

import psycopg2
from psycopg2 import extras 
import pandas as pd
import os
import numpy as np
from datetime import datetime


from google.cloud import bigquery
from google.oauth2 import service_account
import db_dtypes


credentials = service_account.Credentials.from_service_account_file('/home/airflow/gcs/data/googlekey.json')
client = bigquery.Client(credentials=credentials)

#connection to source Postgres DB
conn = psycopg2.connect(
database="oltp", user='postgres', password='Tonsa@1234', host='34.148.192.222', port= '5432')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()
conn.autocommit=True

#default date for initial load
format_data = "%d/%m/%Y %H:%M:%S.%f"
time_data = "01/01/1900 00:00:0.000"
default_date = datetime.strptime(time_data,format_data)


## Function to insert ETL runtime into log

def insertetllog(logid, tblname, rowcount, status):
    try:
        # set record attributes
        record = {"processlogid":logid,"tablename":tblname,"extractrowcount": rowcount,
                  "lastextractdatetime":datetime.now(),"status":status}
        #print(record)
        logs = pd.DataFrame(record, index=[0])
        log_tbl_name = "etlextractlog"
        
        df_columns = list(logs)
        columns = ",".join(df_columns)
        values = "VALUES({})".format(",".join(["%s" for _ in df_columns])) 
        insert_stmt = "INSERT INTO {} ({}) {}".format(log_tbl_name,columns,values)

        cursor.execute(insert_stmt, logs.values)

        conn.commit()
    except Exception as e:
        print("Unable to insert record into etl logs" + print(str(e)))

def getLastETLRunDate(tblName):
    try:
        qry = f"""Select  max(lastextractdatetime) as lastETLRunDate
        from etlextractlog where tablename= '{tblName}'"""
        cursor.execute(qry)
        for i in cursor.fetchall():
            etlrundate = i[0]
            if not etlrundate:
                etlrundate = default_date
            return etlrundate
    except Exception  as e:
        return default_date


# # Get new records from customer and orders table

def CustomerExtract():
    dim_customer_fields = ['customerid','name','address_id','start_date','end_date']
    
    lastrundate = getLastETLRunDate('customer_master')
    df = pd.read_sql(f'''select * from customer_master where update_timestamp >= '{lastrundate}';''',conn)
    return df


def OrdersExtract():
    lastrundate = getLastETLRunDate('order_details')
    df = pd.read_sql(f'''select * from order_details where order_status_update_timestamp >= '{lastrundate}';''',conn)
    return df


def AddressTransform(): 
    dim_address_fields = ['address_id','address','city','state','pincode']
    dim_address = pd.DataFrame(columns = dim_address_fields)
    delta_address = pd.DataFrame(columns = dim_address_fields,index=range(1,len(delta_customer)+1))
    
    
    #fetch hisotrical records
    query_string ="""SELECT *
                    FROM `united-bongo-351617.UFH.dim_address`
                    ORDER BY address_id DESC"""
    dim_addres = (client.query(query_string).result().to_dataframe(create_bqstorage_client=False,))
    
    
    #fetch delta records    
    for i in range(1,len(delta_customer)+1):
        delta_address['address_id'][i] = i
        delta_address['address'][i] = delta_customer['address'][i]
        delta_address['city'][i] = delta_customer['city'][i]
        delta_address['state'][i] = delta_customer['state'][i]
        delta_address['pincode'][i] = delta_customer['pincode'][i]
        
    #compare 
    inserts = delta_address[~delta_address.apply(tuple,1).isin(dim_addres.apply(tuple,1))]
    return inserts


def upsert(df, tbl, key, lastrundate):
    if(lastrundate== default_date):
        try:
            print('Historical Load Started')
            tableRef = client.dataset("UFH").table(tbl)
            bigqueryJob = client.load_table_from_dataframe(df, tableRef)
            #bigqueryJob.result()
            print('Load Completed')
            #insertetllog(1, tbl, len(df), "Completed")
        except Exception as e:  
            insertetllog(1, tbl, len(df), "Failed")
    else:
        try:
            print('Incremental Load Started')
            #update_to_sql(df, tbl, key)
            #insertetllog(1, tbl, len(source), "Completed")
            print('Load Completed')
        except Exception as e:  
            insertetllog(1, tbl, len(source), "Failed")
            
            
delta_customer = CustomerExtract()
#delta_orders = OrdersExtract()
delta_customer.index=np.arange(1,len(delta_customer)+1)
#delta_orders.index=np.arange(1,len(delta_orders)+1)

address_inserts=AddressTransform()
upsert(address_inserts,'dim_address','address_id',default_date)