import csv
from faker import Faker
import datetime
import random
import time
from collections import defaultdict
import string
import pandas as pd
from datetime import timedelta
import psycopg2
from random import randrange
from datetime import datetime

#def datagenerate(records):
fake = Faker('en_IN')


order_received_dict = defaultdict(list)
order_inprogress_dict = defaultdict(list)
order_delivered_dict = defaultdict(list)


records=1000
for i in range(records):
    order_received_dict['orderid'].append(fake.unique.random_int(1,1000)),
    order_received_dict['customerid'].append(fake.random_int(1,1001))
    order_received_dict['order_status_update_timestamp'].append(fake.date_time_this_year())
    order_received_dict['order_status'].append('Received')
    
    #order_inprogress_dict['order_status_update_timestamp'].append(inprogress_time)
    order_inprogress_dict['order_status'].append('in_progress')
    
    #order_delivered_dict['order_status_update_timestamp'].append(delivered_time)
    order_delivered_dict['order_status'].append('Delivered')
                                               

list1=[] 
list2=[]
list3=[]
random_hour=round(random.uniform(2,5),0)
random_min=round(random.uniform(10,20),0)
print(random_min)
print(random_hour)

for val1 in order_received_dict['orderid']:
    list1.append(val1)
    #print(list1)
for val2 in order_received_dict['customerid']:  
    list2.append(val2)
for val3 in order_received_dict['order_status_update_timestamp']:  
    list3.append(val3)
for order in list1:
    order_inprogress_dict['orderid'].append(order)
    order_delivered_dict['orderid'].append(order)
for cus in list2:
    order_inprogress_dict['customerid'].append(cus)
    order_delivered_dict['customerid'].append(cus)
for dates in list3:
    #print(list3)
    order_inprogress_dict['order_status_update_timestamp'].append(dates+timedelta(minutes=random_min))
    order_delivered_dict['order_status_update_timestamp'].append(dates+timedelta(hours=random_hour))
                
                
            
df_received = pd.DataFrame(order_received_dict)
df_inprogress= pd.DataFrame(order_inprogress_dict)
df_delivered= pd.DataFrame(order_delivered_dict)
    
df_add=pd.concat([df_received, df_inprogress, df_delivered], ignore_index=True)
print(df_add)
 
conn = psycopg2.connect(
    database="oltp", user='postgres', password='Tonsa@1234', host='34.148.192.222', port= '5432')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()
    
create_table_order_details = ''' create table IF NOT EXISTS order_details(
                                orderid int not null ,
                                customerid int,
                                order_status_update_timestamp varchar(50),
                                order_status varchar(20))
                                '''
cursor.execute(create_table_order_details)
conn.commit()
  
temp =0
for i in range(len(df_add)):
        orderid = int(df_add['orderid'].iloc[i])
        customerid = str(df_add['customerid'].iloc[i])
        order_status_update_timestamp = str(df_add['order_status_update_timestamp'].iloc[i])
        order_status = str(df_add['order_status'].iloc[i])
        
        
    
        query = ("insert into order_details(orderid, customerid, order_status_update_timestamp, order_status)"
             "values (%s, %s, %s, %s)")

        val = (orderid, customerid, order_status_update_timestamp, order_status)
        cursor.execute(query,val)
        conn.commit()
        temp = temp + 1
        print(temp, "record inserted",orderid)
            
    
