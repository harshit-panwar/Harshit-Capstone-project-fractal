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

fake = Faker('en_IN')

order_dict = defaultdict(list)
records=100
for i in range(records):
    order_dict['orderid'].append(fake.random_int(1,101)),
    order_dict['productid'].append(fake.random_int(1,101))
    order_dict['quantity'].append(fake.random_int(1,6))
    
    
df = pd.DataFrame(order_dict)        
#print(df)
conn = psycopg2.connect(
    database="oltp", user='postgres', password='Tonsa@1234', host='34.148.192.222', port= '5432')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()
    
create_table_order_items = ''' create table IF NOT EXISTS order_items(
                                orderid int not null ,
                                productid int,
                                quantity varchar(50))
                                
                                '''
cursor.execute(create_table_order_items)
conn.commit()
  
temp =0
for i in range(len(df)):
        orderid = int(df['orderid'].iloc[i])
        productid = str(df['productid'].iloc[i])
        quantity = str(df['quantity'].iloc[i])
        
        query = ("insert into order_items(orderid, productid, quantity)"
             "values (%s, %s, %s)")

        val = (orderid, productid, quantity)
        cursor.execute(query,val)
        conn.commit()
        temp = temp + 1
        print(temp, "record inserted",orderid)
            
