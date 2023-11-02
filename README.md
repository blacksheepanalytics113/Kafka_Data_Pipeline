# Kafka_Data_Pipeline

# Project Overview 
For this project i have been cooking up a robust,scalable and fault tolerant data pipeline which has been a complex task involves muiltiple tools and technologies. 
We will explore an end to end data engineering project that uses docker, Apache Airflow,Apache Kafka,Apache spark,Mongo DB or PostgreSQL. 

# Data Stack
1. Apache Airflow: An open source platform used to schedule and monitor workflows

2. Apache Kafka: A distributed streaming platform designed for fault tolerance, high throughput and scalibility

3. Apache Spark: A unified analytics engine for big data processing with built in modules for streaming, SQL Machine learning and graph processing.

4. Docker: A containerization tool that allows for isolated, consistent and easily deployable applications

5. PostgreSQL: Ana open source relational database that focuses on extensibility and SQL compliance

6. Mongo DB:  MongoDB is a popular open-source NoSQL database management system that is designed to store, retrieve, and manage large volumes of data in a flexible and scalable manner.

7. Linux: DigitalOcean Droplets

# Architecture Overview

1. Data Ingestion: Raw Data gotten from API is ingested into the system using Kafka. The data can come from various sources like IOT Devices, user activity etc.

2. Data Processing: Airflow schedules spark jobs to process the raw data. The processed data can either be aggregated, filtered, or transformed based on my business logic

3. Data Storage: The processing data is stored in either Mongo DB or PostgreSQL for relational data storage.
  
4. Orchestartion: Docker container encapsulate each component of the architecture , ensuring isolation and ease of deployment.
