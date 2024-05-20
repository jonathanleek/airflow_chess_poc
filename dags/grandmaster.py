doc_md_DAG = """
### grandmaster

A dag that administrates the tournament as a whole. 
	• If no bracket dictionary exists, Reads a json of participants from the S3 bucket, and generates the Bracket Dictionary.
	• If Bracket Dictionary exists, generate any non-existent Game DAGs and trigger them
"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from include.create_tournament_bracket import create_and_upload_tournament_bracket
from include.retrieve_json_from_s3 import retrieve_json_from_s3
from include.process_to_be_played_matches import process_to_be_played_matches
bucket = ''
aws_conn_id = ''

def bracket_branch_function(**kwargs):
    ti = kwargs['ti']
    bracket_exists = ti.xcom_pull(task_ids='check_for_bracket')
    if bracket_exists:
        return 'process_to_be_played_matches'
    else:
        return 'get_participants_json'


with DAG(
    start_date=datetime(2024, 5, 17),
    max_active_runs=1,
    schedule_interval=None,
    doc_md=doc_md_DAG,
) as dag:

    check_for_participants_json = S3KeySensor(
        task_id='check_for_participants_json',
        bucket_key='participants.json',
        bucket_name='bucket',
        aws_conn_id=aws_conn_id,
        timeout=60,
        poke_interval=5
    )

    check_for_bracket = S3KeySensor(
        task_id='check_for_bracket',
        bucket_key='bracket.json',
        bucket_name='bucket',
        aws_conn_id=aws_conn_id,
        timeout=60,
        poke_interval=5
    )

    bracket_creation_branch = BranchPythonOperator(
        task_id='bracket_creation_branch',
        python_callable=bracket_branch_function,
        provide_context=True
    )

    get_participants_json = PythonOperator(
        task_id='get_participants_json',
        python_callable=retrieve_json_from_s3,
        op_kwargs={
            'bucket_name': bucket,
            'key': 'participants.json',
            'output_key': 'participants'
        }
    )

    generate_bracket_json = PythonOperator(
        task_id='generate_bracket_json',
        python_callable=create_and_upload_tournament_bracket,
        op_kwargs={
            'participants_json': "{{ ti.xcom_pull(task_ids='get_participants_json', key='participants_json') }}",
            'bucket_name': bucket
        }
    )

    process_matches = PythonOperator(
        task_id='process_to_be_played_matches',
        python_callable=process_to_be_played_matches,
        op_kwargs={
            'bucket_name': 'bucket',
            'key': 'bracket.json',
        }
    )


check_for_participants_json >> check_for_bracket >> bracket_creation_branch
    bracket_creation_branch >> get_participants_json >> generate_bracket_json
    bracket_creation_branch >> process_to_be_played_matches