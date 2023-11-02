import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import logging
# from cassandra.cluster import Cluster
import pymongo
from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col
# from pyspark.sql.types import StructType, StructField, StringType
from Get_Data_dag import  get_data
# from Get_Data_dag import format_data
import logging
from time import sleep
import json
import psycopg2




def create_connection_PostgreSQL():
    print('Connecting to the PostgreSQL database...')
    try:
            conn = psycopg2.connect(
                host="xxxxxxxxxxxxxxxxx",
                database="xxxxxxxxxxxxxx",
                user="xxxxxxxxxx",
                password="xxxxxxxxxxxxx",
                port=25060
            )

            cur = conn.cursor()   
            json_object = get_data()
            print(json_object)
            
            for data in json_object:
                data = {}
                res = get_data()
                location = res['location']
                # print(location)
                data['id'] = uuid.uuid4()
                # print(data['id'])
                dob = res['dob']['age']
                id = res['login']['uuid']
                # print(id)
            

                # # for list in res:
                cur.execute('SELECT * from "User_API"  where "Firstname" = %s',[res['name']['first']])
                User_Data = cur.fetchall()
                # print(User_Data)
                if len(User_Data) == 0:
                        cur.execute('Insert Into "User_API" ("Id","Firstname","Lastname", "Gender","Address","Post_code","Email","Username","Registered_Date","Phone","Age") values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',(res['login']['uuid'],res['name']['first'],res['name']['last'],(res['gender']),res['location']['street']['name'],res['location']['postcode'],res['email'],res['login']['username'],res['registered']['date'],res['phone'],res['dob']['age']))
                        conn.commit()
                        print(res['name']['last'])
            conn.commit()
            # close the communication with the PostgreSQL
            cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')     
create_connection_PostgreSQL()
