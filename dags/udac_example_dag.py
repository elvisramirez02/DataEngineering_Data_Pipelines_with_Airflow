from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (CreateTableOperator, StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)

from helpers import sql_queries

#Variables
s3_bucket = 'dend-udacity'
s3_song_key = "song_data"
s3_log_key = "log_data"
log_file_json = "log_path_json.json"



default_args = {
    'owner': 'udacity',
    'depends_on_past':False,
    'retries': 0,
    'catchup':False,
    'retry_delay': timedelta(minutes=5), 
    'start_date': datetime(2019, 1, 12),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag_name = 'udac_example_dag' 

dag = DAG('s3_to_redshift_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Apache Airflow',
          schedule_interval='0 * * * *',
          max_active_runs = 1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_in_redshift = CreateTableOperator(
    task_id = 'create_tables_in_redshift',
    redshift_conn_id = 'redshift',
    dag = dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    tableName="stage.staging_events",
    s3_bucket=s3_bucket,
    s3_key=s3_log_key, 
    file_format="JSON",
    log_json_file = log_file_json,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    dag=dag
    
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    tableName="stage.staging_songs",
    s3_bucket=s3_bucket,
    s3_key=s3_log_key, 
    file_format="JSON",
    log_json_file = log_file_json,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift",
    table="stage.songplays",
    truncate_table=True,
    query=sql_queries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id="redshift",
    table="stage.users",
    truncate_table=True,
    query=sql_queries.user_table_insert,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="stage.songs",
    truncate_table=True,
    query=sql_queries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id="redshift",
    table="stage.artists",
    truncate_table=True,
    query=SqlQueries.artist_table_insert,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    table="stage.time",
    truncate_table=True,
    query=sql_queries.time_table_insert,
    dag=dag
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
        dag=dag,
    tables = ["stage.artists", "stage.songplays", "stage.songs", "stage.time", "stage.users"]
    
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables_in_redshift
create_tables_in_redshift  >>  [stage_events_to_redshift, stage_songs_to_redshift] >>  load_songplays_table
load_songplays_table >> [ load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator