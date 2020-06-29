# Project 4: Data Lakes with Spark

## Summary

A startup called **Sparkify** has grown its user base and song database significantly, and the company wants to move their data warehouse to a data lake. 

The goal of this project is create a data lake and an ETL pipeline in Spark that loads data from S3, processes the data into analytics tables, and loads them back into S3.

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

* `dl.cfg` contains AWS access keys
* `etl.py` contains ETL processes

## How to Run

Add AWS access keys in `dl.cfg`

```
[AWS]
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
```

If running locally, type the following command in your terminal

```
python etl.py
```
