# Project 5: Data Pipelines with Airflow

## Summary

A startup called **Sparkify** wants to analyze data on their songs and user activity on their music streaming app. However, their data exists in a directory of JSON logs in S3, which makes it difficult for analysts to use. 

The goal of this project is create ETL pipelines using Apache Airflow to copy data from S3 into Redshift and create a data warehouse, with fact and dimension tables, for the company to easily use.

## ETL Overview

![DAG](airflow-dag.png)

The DAG is made up of the following steps:
1. Copy data from S3 to staging tables in Redshift (using `StageToRedshiftOperator`)
2. Create fact table (using `LoadFactOperator`)
3. Create dimension tables (using `LoadDimensionOperator`)
4. Perform data quality checks (using `DataQualityOperator`)

