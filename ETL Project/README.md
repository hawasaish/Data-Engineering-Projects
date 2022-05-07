# ETL_Project
In this ETL project, task is to build a batch ETL pipeline to read transactional data from RDS, transform and load it into target dimensions and facts on Redshift. 
#### The tasks performed in this project are:
- Extracting the transactional data from a given MySQL RDS server to HDFS instance using Sqoop.

- Transforming the transactional data according to the given target schema using PySpark. 

- Transformed data is to be loaded to an S3 bucket.

- Creating the Redshift tables according to the given schema.

- Loading the data from Amazon S3 to Redshift tables.

- Performing the analysis queries.