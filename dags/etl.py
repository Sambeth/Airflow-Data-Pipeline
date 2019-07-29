from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from helpers import SqlQueries
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 7, 29),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'depends_on_past': False
}

dag = DAG(
    'etl_process',
    default_args=default_args,
    schedule_interval='@hourly'
)

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="stage_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket='udacity-dend',
    s3_key="log_data/",
    copy_options=["format as json 's3://udacity-dend/log_json_path.json'"],
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket='udacity-dend',
    s3_key="song_data",
    copy_options=["json 'auto' compupdate off region 'us-west-2'"],
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='load_songplays_fact_table',
    redshift_conn_id="redshift",
    table="songplays",
    data_source=SqlQueries.songplay_table_insert,
    dag=dag
)

dim_tables_and_sources = [
    ("users", SqlQueries.user_table_insert),
    ("songs", SqlQueries.song_table_insert),
    ("artists", SqlQueries.artist_table_insert),
    ("time", SqlQueries.time_table_insert),
]

load_dimension_tables = LoadDimensionOperator(
    task_id='load_dim_tables',
    redshift_conn_id="redshift",
    tables=dim_tables_and_sources,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='run_data_quality_checks',
    redshift_conn_id="redshift",
    tables=dim_tables_and_sources,
    dag=dag
)

end_operator = DummyOperator(task_id='stop_execution', dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_dimension_tables
load_dimension_tables >> run_quality_checks
run_quality_checks >> end_operator
