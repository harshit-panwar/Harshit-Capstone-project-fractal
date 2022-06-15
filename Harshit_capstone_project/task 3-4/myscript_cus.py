import os
import numpy as np
from datetime import datetime
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import db_dtypes
from pandas.io import gbq
import psycopg2
import time


#credentials = service_account.Credentials.from_service_account_file('/home/airflow/gcs/data/googlekey.json')
#client = bigquery.Client(credentials=credentials)

#connection to source Postgres DB
conn = psycopg2.connect(
database="oltp", user='postgres', password='Tonsa@1234', host='34.148.192.222', port= '5432')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()
conn.autocommit=True

#extracting dim_customer
df_customer=pd.read_sql(''' SELECT customerid,name,row_number() over(order by customerid asc) as address_id,
            update_timestamp as start_date,'null' as end_date FROM customer_master; ''' , conn)
            

#extracting dim_product
todaysdate = time.strftime("%d.%m.%Y")
dim_product= pd.read_sql(''' select product_id,product_code,product_name,SKU,rate,is_active, now() as start_date, 
                'null' as end_date from product_master;''' , conn)

#extracting dim_order               
dim_order=pd.read_sql(''' select orderid,order_status_update_timestamp,order_Status from order_details;''', conn)
#print(dim_order)

#extracting dim_address
dim_address=pd.read_sql(''' select row_number() over(order by customerid asc) as address_id,address,city,state,pincode
                        from customer_master;''' , conn)
#print(dim_address)

#extracting fact_order_details
f_order_details= pd.read_sql(''' select oi.orderid,od.order_status_update_timestamp
                            as order_delivery_timestamp, oi.productid , oi.quantity from order_items oi, order_details od
                           where od.orderid=oi.orderid and od.order_status = 'Delivered';''' ,conn)

          

credentials = service_account.Credentials.from_service_account_file('/home/airflow/gcs/data/capstone-8-352804-0d4d554372d4.json')
client = bigquery.Client(credentials=credentials)

df_customer.to_gbq(destination_table='star_schema.dim_customer',project_id='capstone-8-352804',if_exists='append')
dim_product.to_gbq(destination_table='star_schema.dim_product',project_id='capstone-8-352804',if_exists='append')
dim_order.to_gbq(destination_table='star_schema.dim_order',project_id='capstone-8-352804',if_exists='append')
dim_address.to_gbq(destination_table='star_schema.dim_address',project_id='capstone-8-352804',if_exists='append')
f_order_details.to_gbq(destination_table='star_schema.f_order_details',project_id='capstone-8-352804',if_exists='append')







