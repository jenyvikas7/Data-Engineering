# Data Pipelines with Airflow
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of CSV logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Project Setup

1. Create a Redshift cluster with IAM role to be able to access S3.
2. Create Postgres connection using Admin screen and provide Host, DB,port, userid and password
3. Create AWS connection using Admin screen and provide Access ID and Secret Key for a user who has appropriate access to Redshift and S3 service.

**AWS Connection**
Conn Id: Enter aws_credentials.
Conn Type: Enter Amazon Web Services.
Login: Enter your Access key ID from the IAM User credentials you downloaded earlier.
Password: Enter your Secret access key from the IAM User credentials you downloaded earlier.

**Redshift Connection**
Conn Id: Enter redshift.
Conn Type: Enter Postgres.
Host: Enter the endpoint of your Redshift cluster, excluding the port at the end. 
Schema: This is the Redshift database you want to connect to.
Login: Enter awsuser.
Password: Enter the password created when launching the Redshift cluster.
Port: Enter 5439.

## Data Source 
* Log data: `s3://udacity-dend/log_data`
* Song data: `s3://udacity-dend/song_data`

## Project Strucute
* README: Current file, holds instructions and documentation of the project
* dags/sparkify_dag.py: Directed Acyclic Graph definition with imports, tasks and task dependencies
* plugins/helpers/sql_queries.py: Contains Insert SQL statements
* plugins/operators/create_tables.sql: Contains SQL Table creations statements
* plugins/operators/create_tables.py: CreateTablesOperator that create all required tables
* plugins/operators/stage_redshift.py: StageToRedshiftOperator that copies data from S3 buckets into redshift staging tables
* plugins/operators/load_dimension.py: LoadDimensionOperator that loads data from redshift staging tables into dimensional tables
* plugins/operators/load_fact.py: LoadFactOperator that loads data from redshift staging tables into fact table
* plugins/operators/data_quality.py: DataQualityOperator that validates data quality in redshift tables
* plugins/operators/createtables.sql: File with sql queries to create all required tables


### Sparkify DAG
DAG parameters:

* The DAG does not have dependencies on past runs
* DAG has schedule interval set to hourly
* On failure, the task retries 3 times
* Retry interval is 5 minutes
* Catchup is turned off


DAG contains default_args dict bind to the DAG, with the following keys:
   
    * Owner
    * Depends_on_past
    * Start_date
    * Retries
    * Retry_delay
    * Catchup

* Task dependencies are set as following:

![DAG Flow](imgs/airflow-pipeline.PNG)

### Operators
Operators create necessary tables, stage the data, transform the data, and run checks on data quality.

Connections and Hooks are configured using Airflow's built-in functionalities.


#### Stage Operator
The stage operator loads any JSON and CSV formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters should specify where in S3 the file is loaded and what is the target table.


#### Fact and Dimension Operators
The dimension and fact operators make use of the SQL helper class to run data transformations. Operators take as input the SQL statement from the helper class and target the database on which to run the query against. A target table is also defined that contains the results of the transformation.

Dimension loads are done with the truncate-insert pattern where the target table is emptied before the load. There is a parameter that allows switching between insert modes when loading dimensions. Fact tables are massive so they only allow append type functionality.


#### Data Quality Operator
The data quality operator is used to run checks on the data itself. The operator's main functionality is to check the quality of data by running SQL test cases along with the expected results. For each the test, the test result and expected result are checked and if there is a mismatch, the operator raises an exception.

For example one test could be a SQL statement that checks if certain column contains NULL values by counting all the rows that have NULL in the column. We do not want to have any NULLs so expected result would be 0 and the test would compare the SQL statement's outcome to the expected result.


### Airflow UI views of DAG and plugins

The dag follows the data flow provided in the instructions, all the tasks have a dependency and DAG begins with a start_execution task and ends with a end_execution task.