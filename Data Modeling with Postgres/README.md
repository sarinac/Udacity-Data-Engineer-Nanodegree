# Project 1: Data Modeling with Postgres

## Summary

A startup called **Sparkify** wants to analyze data on their songs and user activity on their music streaming app. However, their data exists in a directory of JSON logs, which makes it difficult for analysts to use. 

The goal of this project is to create a Postgres database by building an ETL pipeline using Python. 

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

* `test.ipynb` displays the first few rows of each table to check records in the database.
* `create_tables.py` drops and creates tables.
* `etl.ipynb` reads and processes a single file from `song_data` and `log_data` and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.
* `etl.py` reads and processes files from `song_data` and `log_data` and loads them into your tables. 
* `sql_queries.py` contains all sql queries, and is imported into the last three files above.
* `README.md` provides summary of project.

## Instructions

Copy the following command in your terminal to create the `Sparkify` database.
```
python create_tables.py
```

Copy the following command in your terminal to insert JSON files in the `data` directory into the database.
```
python etl.py
```

Run `test.ipynb` to test whether data successfully loaded in the tables.
