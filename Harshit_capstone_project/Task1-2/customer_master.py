
import csv
from faker import Faker
import datetime
import random
import pandas as pd
from collections import defaultdict
import psycopg2
import time

def datagenerate(records):
    fake = Faker('en_IN')
    cities = {'Rajasthan':['Jaipur','Udaipur'],
              'Maharashtra':['Mumbai','Pune','Aurangabad','Nagpur','Nashik'],
              'Gujarat':['Ahemdabad','Surat','Vadodara','Gandhinagar','Rajkot'],
              'Karnataka':['Bengaluru','Mangaluru','Hubbali','Mysuru'],
              'Madhya Pradesh':['Bhopal','Indore'],
              'Uttar Pradesh':['Kanpur','Lucknow', 'Varanasi', 'Agra']
             }
    
    fake_data = defaultdict(list)
    
    for i in range(records):
            St= random.choice(list(cities.keys()))
            #random_datetime = randomDate("20-01-2021 13:30:00", "30-08-2022 04:50:34")       
            fake_data['customerid'].append(fake.unique.random_int(1,1001))
    #fake.unique.clear()
            fake_data['name'].append(fake.name())
            fake_data['address'].append(fake.street_address())
            fake_data['city'].append(random.choice(list(cities[St])))
            fake_data['state'].append(St)
            fake_data['pincode'].append(fake.postcode())
            fake_data['update_timestamp'].append(fake.date_time_this_decade())
     
    df = pd.DataFrame(fake_data)        
    print(df)
    
    conn = psycopg2.connect(
    database="oltp", user='postgres', password='Tonsa@1234', host='34.148.192.222', port= '5432')
#Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    
    create_table = ''' create table IF NOT EXISTS customer_master(
                                customerid int not null primary key,
                                name varchar(50),
                                address varchar(100),
                                city varchar(30),
                                state varchar(30),
                                pincode int,
                                update_timestamp timestamp)'''
    cursor.execute(create_table)
    conn.commit()

    temp =0
    for i in range(len(df)):
        customerid = int(df['customerid'].iloc[i])
        name = str(df['name'].iloc[i])
        address = str(df['address'].iloc[i])
        city = str(df['city'].iloc[i])
        state = str(df['state'].iloc[i])
        pincode = int(df['pincode'].iloc[i])
        update_timestamp = str(df['update_timestamp'].iloc[i])
    
        query = ("insert into customer_master(customerid,name, address, city, state, pincode, update_timestamp)"
             "values (%s, %s, %s, %s, %s, %s, %s)")

        val = (customerid,name, address, city, state, pincode, update_timestamp)
        cursor.execute(query,val)
        conn.commit()
        temp = temp + 1
        print(temp, "record inserted",customerid)
        
if __name__ == '__main__':
    records = 1000
  
    datagenerate(records)
    print("CSV generation complete!")
   
