import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import json
from kafka import KafkaProducer
import logging
from time import sleep
from pykafka import KafkaClient
import threading



def get_data():
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    # print(res)
    ress = res['results'][0]
    # ress = json.dumps(ress,indent = 4)
    return ress
get_data()


def format_data():
    data = {}
    res = get_data()
    location = res['location']
    # print(location)
    data['id'] = uuid.uuid4()
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    print(data['gender'])
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    print( data['picture'])

    print(data)
format_data()



def stream_data():
    Kafka_host = "137.184.109.96:9092"
    client = KafkaClient(hosts=Kafka_host)
    topic = client.topics["stream_data"]
    
    with topic.get_sync_producer() as producer:
        for i in range(10):
            message = get_data() 
            # message_1 = format_data()
            encoded_message = json.dumps(message).encode("utf-8")
            producer.start()
            producer.produce(encoded_message)
            
            sleep(0.5)
           
            producer.stop()
            
stream_data()

