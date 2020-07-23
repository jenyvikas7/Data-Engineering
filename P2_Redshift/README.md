# Project Data Warehouse(DWH)

## Project description

Sparkify is a music streaming startup with a growing user base and song database.Their user activity and songs metadata data resides in json files in S3. The goal of the current project is to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. 

## Steps to execute the project

1. Save the informatioon related to Redshift cluster, IAM Role, AWS keys and S3 buckets in dwh.cfg file.

```
[CLUSTER]
HOST=''
DB_NAME=''
DB_USER=''
DB_PASSWORD=''
DB_PORT=5439

[IAM_ROLE]
ARN=

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'

[DWH]
DWH_CLUSTER_TYPE       = multi-node
DWH_NUM_NODES          = 2
DWH_NODE_TYPE          = dc2.large
DWH_CLUSTER_IDENTIFIER = 
DWH_DB                 = 
DWH_DB_USER            = 
DWH_DB_PASSWORD        = 
DWH_PORT               = 5439
DWH_IAM_ROLE_NAME      = 

[AWS]
KEY=
SECRET=
```
2. Run the *create_cluster* script to set up the Redshift cluster for this project.

    `%run create_cluster.py`

3. Run the *get_cluster_details* script to get the host address once Redshift Cluster is available

    `%run get_cluster_details.py`

4. Run the *create_tables* script to drop and create the required tables.

    `%run create_tables.py`
    
5. Run the *etl* script to extract data from the files in S3, stage it into redshift, and finally insert it into the dimensional tables.

    `%run etl.py`
    
6. Run the *test* script to query the record counts in each Redshift table.

    `%run test.py`


## Project structure

This project has six script files:

- create_cluster.py which programatically creates Redshift cluster in AWS
- create_tables.py which creates the tables required in the project
- etl.py is where data gets loaded from S3 into staging tables on Redshift and then processed into the analytics tables on Redshift.
- sql_queries.py where DDL statements are defined, which are then used create_tables.py to create the required tables
- get_cluster_details.py retrieves the host address from the Redshift cluster once it is availabe and then updates this value in dwh.cfg file
- test.py has the sql queries to get the record counts from each table

## Database schema design

#### Staging Tables
- staging_events
- staging_songs

####  Fact Table
- songplays - records in event data associated with song plays i.e. records with page NextSong
*songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

#### Dimension Tables
- users - users in the app - 
*user_id, first_name, last_name, gender, level*
- songs - songs in music database - 
*song_id, title, artist_id, year, duration*
- artists - artists in music database - 
*artist_id, name, location, lattitude, longitude*
- time - timestamps of records in songplays broken down into specific units - 
*start_time, hour, day, week, month, year, weekday*


## Queries and Results

Number of rows in each table:

-SELECT COUNT(*) FROM staging_events retuns 8056 records

-SELECT COUNT(*) FROM staging_songs retuns 14896 records

-SELECT COUNT(*) FROM songplays retuns 333 records

-SELECT COUNT(*) FROM users retuns 104 records

-SELECT COUNT(*) FROM songs retuns 14896 records

-SELECT COUNT(*) FROM artists retuns 10025 records

-SELECT COUNT(*) FROM time retuns 333 records