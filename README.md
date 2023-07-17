# Project: Data Pipelines
In this project I've configured and scheduled data pipelines with **Apache Airflow** to extract raw data from **S3** to **Redshift**, transform and load data from staging tables to dimensional tables.

## How to run the project
1. Create a Redshift cluster in us-west-2 region
2. Turn on Airflow by running Airflow/start.sh
3. Add tables using queries fro create_tables.sql
4. Create AWS and Redshift connections on Airflow Web UI
5. Run udac_example _dag DAG to trigger ETL data pipeline