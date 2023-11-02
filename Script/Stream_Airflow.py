from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable
from Dags.Get_Data_dag import get_data
from Dags.Get_Data_dag import format_data
from Dags.Get_Data_dag import stream_data
from Dags.Save_Db_PostgreSQL import create_connection_PostgreSQL
from Dags.Save_db_Mongo import create_connection_Mongo


with DAG("Stream_Api_Data", start_date=datetime(2023, 10, 27),
schedule_interval='*/15 * * * *', max_active_runs=1, catchup=False) as dag:

    Kafka_script_A = PythonOperator(
        task_id="Get_Api_Data",
        python_callable=get_data
    )

    Kafka_script_A
    
    Kafka_script_B = PythonOperator(
        task_id="Transform_data",
        python_callable=format_data
    )

    Kafka_script_B
    
    Kafka_script_C = PythonOperator(
        task_id="Stream_Kafka",
        python_callable=stream_data
    )

    Kafka_script_C
    
    Kafka_script_D = PythonOperator(
        task_id="Stream_Kafka",
        python_callable=create_connection_Mongo
    )

    Kafka_script_D
    
    
    Kafka_script_A >> Kafka_script_B >> Kafka_script_C
    
    