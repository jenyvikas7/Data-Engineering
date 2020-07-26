from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries



default_args = {
    'owner': 'udacity',
    'start_date': datetime(2020, 7, 26),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=300),
    'catchup': False
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_path="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    select_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    truncate_table=True,
    select_query=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    truncate_table=True,
    select_query=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    truncate_table=True,
    select_query=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    truncate_table=True,
    select_query=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
   task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=[
        "songplays",
        "users",
        "songs",
        "artists",
        "time"
    ],
    dq_checks=[
            {'check_sql':'SELECT COUNT(*) FROM songplays' , 'expected_result': 47740},
            {'check_sql':'SELECT COUNT(*) FROM users' , 'expected_result': 104},
            {'check_sql':'SELECT COUNT(*) FROM songs' , 'expected_result': 14896},
            {'check_sql':'SELECT COUNT(*) FROM artists' , 'expected_result': 10025},
            {'check_sql':'SELECT COUNT(*) FROM time' , 'expected_result': 47740}
]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Setting up the dependencies and order
start_operator >> [stage_events_to_redshift,stage_songs_to_redshift]
[stage_events_to_redshift,stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_song_dimension_table,load_user_dimension_table,load_artist_dimension_table,load_time_dimension_table]
[load_song_dimension_table,load_user_dimension_table,load_artist_dimension_table,load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator
