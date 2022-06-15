
import csv
from faker import Faker
import datetime
import random
import pandas as pd
from collections import defaultdict
import psycopg2
import string
import time


#product_master


def datagenerate(records):
    fake = Faker('en_IN')
    Product_comp = ['Aashirwad','Tata','Reliance','Balaji','MDH','dabur','patanjali','Amul']
    Product_cat= ['Soap','oil','sugar','aata','salt','cream','shampoo','Ghee']
    
    
        
    def id_generator(size=2, chars=string.ascii_uppercase + string.ascii_uppercase):
        return ''.join(random.choice(chars) for _ in range(size))
        
    order_dict = defaultdict(list)

    
    for i in range(records):
           # St= random.choice(list(cities.keys()))
            
            order_dict['product_id'].append(fake.unique.random_int(1,101)),
            order_dict['product_code'].append(id_generator())
            order_dict['product_name'].append(random.choice(list(Product_comp)) + ' ' + random.choice(list(Product_cat)))
            order_dict['SKU'].append(str(fake.random_int(1,8)) + 'KG')
            order_dict['Rate'].append(fake.random_int(50,501))
            order_dict['is_active'].append(True)
     
    df = pd.DataFrame(order_dict)        
    print(df)
                     
    conn = psycopg2.connect(
    database="oltp", user='postgres', password='Tonsa@1234', host='34.148.192.222', port= '5432')
#Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    
    create_table_product_master = ''' create table IF NOT EXISTS product_master(
                                product_id int not null primary key,
                                product_code varchar(100),
                                product_name varchar(100),
                                SKU varchar(100),
                                Rate varchar(100),
                                is_active varchar(10))  '''
    cursor.execute(create_table_product_master)
    conn.commit()
    
    temp =0
    for i in range(len(df)):
        product_id = int(df['product_id'].iloc[i])
        product_code = str(df['product_code'].iloc[i])
        product_name = str(df['product_name'].iloc[i])
        SKU = str(df['SKU'].iloc[i])
        Rate = str(df['Rate'].iloc[i])
        is_active = int(df['is_active'].iloc[i])
        
    
        query = ("insert into product_master(product_id, product_code, product_name, SKU, Rate, is_active)"
             "values (%s, %s, %s, %s, %s, %s)")

        val = (product_id, product_code, product_name, SKU, Rate, is_active)
        cursor.execute(query,val)
        conn.commit()
        temp = temp + 1
        print(temp, "record inserted",product_id)
            
    
if __name__ == '__main__':
    records = 50
    
    datagenerate(records)
    print("CSV generation complete!")
