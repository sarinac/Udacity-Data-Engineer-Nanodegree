# Project 3: Cloud Data Warehouse with Redshift

## Summary

A startup called **Sparkify** wants to analyze data on their songs and user activity on their music streaming app. However, their data exists in a directory of JSON logs in S3, which makes it difficult for analysts to use. 

The goal of this project is to create a database in the Cloud by setting up the Redshift host and building an ETL pipeline using Python. 

## Schema 

Fact Table

* `songplays` - records in log data associated with song plays 

Dimension Tables

* `users` - users in the app
* `songs` - songs in music database
* `artists` - artists in music database
* `time` - timestamps of records in `songplays` broken down into specific units

![ERD](Song_ERD.png)

## Project files

* `view_s3_data.ipynb` displays samples of JSON files from S3 in table format. 
* `dwh.cfg` contails all configuration data (access keys, cluster settings, S3 bucket/keys)
* `sql_queries.py` contains all sql queries, and is imported into `create_tables.py`  and `etl.py`.
* `create_tables.py` drops and creates tables.
* `etl.py` copies `song_data` and `log_data` from S3 into staging tables. Then inserts into fact and dimension tables by querying from staging tables.
* `create_db.ipynb` creates the Redshift cluster, runs SQL queries against the database, and deletes the cluster.
* `README.md` provides summary of project.

## Instructions

Run `create_db.ipynb` which will perform the following: 

1. Create and connect to the Redshift cluster. 
2. Run `create_tables.py` and `etl.py` to setup the database. 
3. Run sample queries to check if the ETL was successful. 
4. Delete the cluster.
