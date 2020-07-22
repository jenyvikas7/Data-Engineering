# Sparkify Postgres ETL Project

This is the my project submission for the Data Engineering.
This project is built using below concepts:
- Data modeling with Postgres
- Star Schema
- ETL using Python

## Context

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project, we'll apply what we've learned on data modeling with Postgres and build an ETL pipeline using Python.

### Data
- **Song datasets**: all json files are nested in subdirectories under */data/song_data*. A sample of this files is:

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

- **Log datasets**: all json files are nested in subdirectories under */data/log_data*. A sample of a single row of each files is:

```
{"artist":"Slipknot","auth":"Logged In","firstName":"Aiden","gender":"M","itemInSession":0,"lastName":"Ramirez","length":192.57424,"level":"paid","location":"New York-Newark-Jersey City, NY-NJ-PA","method":"PUT","page":"NextSong","registration":1540283578796.0,"sessionId":19,"song":"Opium Of The People (Album Version)","status":200,"ts":1541639510796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"20"}
```

## Database Schema
The schema used for this exercise is the Star Schema: 
There is one main fact table and 4 dimentional tables, each with a primary key that is being referenced from the fact table.

We chose Relational model here as the data is structured and we can leverage the power of sql to query the data. Also  3Vs of this data suggest that this is not big data.

#### Fact Table
**songplays** - records in log data associated with song plays i.e. records with page NextSong

#### Dimension Tables
**users** - users in the app

**songs** - songs in music database

**artists** - artists in music database

**time** - timestamps of records in songplays broken down into specific units


## Project structure

Files used on the project:
1. **data** folder nested at the home of the project, where all needed jsons reside.
2. **create_tables.py** drops and creates tables. You run this file to reset your tables before each time you run your ETL scripts.
3. **sql_queries.py** contains all your sql queries, and is imported into the files bellow.
4. **test.ipynb** displays the first few rows of each table to let you check your database.
5. **etl.ipynb** reads and processes a single file from song_data and log_data and loads the data into your tables. 
6. **etl.py** reads and processes files from song_data and log_data and loads them into your tables. 
7. **README.md** current file, provides discussion on my project.


## Steps followed

1º Wrote queries for dropping , creating and inserting in sql_queries.py

2º Run create_tables.py to drop and recreate the tables

3º Completed etl.ipynb Notebook to create the blueprint of the pipeline to process and insert all data into the tables.

4º Tested the etl.ipynb using test.ipynb.

5º Ran etl.py from runner.ipynb, and verified the results:

## Author

* **Vikas Jain** - [Github](https://github.com/jenyvikas7/Data-Engineering)