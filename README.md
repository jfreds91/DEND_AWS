# Data Engineering Nanodegree
# Project: AWS ETL

Jesse Fredrickson

12/15/19

## Purpose
The purpose of this project is to set up an ETL (Extract, Transform, Load) pipeline to transport data from an Amazon S3 bucket into fact and dimension tables stored on an Amazon Redshift cluster.

## Method
All of the operations in this project are executed within a framework of a few python files, including both IaC (Infrastructure as Code) statements to start up and configure Redshift with boto3 as well as SQL statements executed by psycopg2. The S3 bucket, owned by Udacity, contains all of the source json files that are to be read into Redshift. The Redshift cluster that gets spun up is mine.

## Files
- **create_tables.py:** Contains main logic for creating Redshift cluster and initializing SQL tables
- **etl<i></i>.py:** Contains main logic for ETL pipeline to populate SQL tables from json files
- **sql_queries.py:** Contains all of the SQL statements for creating and populating SQL tables
- **dwg.cfg:** Contains configuration info for AWS
- **secret.cfg:** (NOT INCLUDED) contains configuration info for user KEY and SECRET

## Usage
Ensure that both configuration files exist and are populated correctly. Create a python environment with psycopg2 and boto3, and use it to run create_tables.py. Once the Redshift cluster has been created, you will need to run create_tables.py again to create the SQL tables within the cluster - it will output print statements to tell you what it is doing. After the tables have been created, run etl<i></i>.py. When you are done using the data, you will have to delete the Redshift cluster from the AWS console.

## Schema
songplays is the fact table, the rest are all dimension tables

![](schema.png)