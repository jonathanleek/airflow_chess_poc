import json
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.trigger_rule import TriggerRule

from include.retrieve_json_from_s3 import retrieve_json_from_s3
from include.process_to_be_played_matches import process_to_be_played_matches
from include.create_and_trigger_player_dags import create_and_trigger_player_dags
from include.create_tournament_bracket import create_and_upload_tournament_bracket

aws_conn_id = 'aws_default'
bucket_name = "airflow-chess"
repo_url = "https://github.com/jonathanleek/airflow_chess_poc"

def bracket_branch_function(**kwargs):
    ti = kwargs['ti']
    bracket_exists = ti.xcom_pull(task_ids='check_for_bracket')
    if bracket_exists:
        return 'process_to_be_played_matches'
    else:
        return 'get_participants_json'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

with DAG(
    dag_id='grandmaster',
    default_args=default_args,
    description='A dag that administrates the tournament as a whole.',
    schedule_interval=None,
    catchup=False,
) as dag:

    check_for_participants_json = S3KeySensor(
        task_id='check_for_participants_json',
        bucket_key='participants.json',
        bucket_name=bucket_name,
        aws_conn_id=aws_conn_id,
        timeout=30,
        poke_interval=5
    )

    check_for_bracket = S3KeySensor(
        task_id='check_for_bracket',
        bucket_key='bracket.json',
        bucket_name=bucket_name,
        aws_conn_id=aws_conn_id,
        timeout=30,
        poke_interval=5
    )

    bracket_creation_branch = BranchPythonOperator(
        task_id='bracket_creation_branch',
        python_callable=bracket_branch_function,
        provide_context=True,
        trigger_rule='all_done'
    )

    get_participants_json = PythonOperator(
        task_id='get_participants_json',
        python_callable=retrieve_json_from_s3,
        op_kwargs={
            'bucket_name': bucket_name,
            'key': 'participants.json',
            'output_key': 'participants'
        }
    )

    generate_bracket_json = PythonOperator(
        task_id='generate_bracket_json',
        python_callable=create_and_upload_tournament_bracket,
        trigger_rule='all_done',
        op_kwargs={
            'participants_json': "{{ ti.xcom_pull(task_ids='get_participants_json', key='participants') }}",
            'bucket_name': bucket_name
        }
    )

    process_matches = PythonOperator(
        task_id='process_to_be_played_matches',
        python_callable=process_to_be_played_matches,
        op_kwargs={
            'bucket_name': bucket_name,
            'key': 'bracket.json',
        }
    )

    trigger_player_dags = PythonOperator(
        task_id='trigger_player_dags',
        python_callable=create_and_trigger_player_dags,
        op_kwargs={
            'bucket_name': bucket_name,
            'key': 'bracket.json',
            'repo_url': repo_url
        },
        provide_context=True
    )

    check_for_participants_json >> check_for_bracket >> bracket_creation_branch
    bracket_creation_branch >> get_participants_json >> generate_bracket_json
    bracket_creation_branch >> process_matches >> trigger_player_dags
    generate_bracket_json >> process_matches
