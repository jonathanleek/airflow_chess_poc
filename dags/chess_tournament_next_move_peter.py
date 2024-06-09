import json
import chess
import chess.engine
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

def retrieve_json_from_s3(bucket_name, key, output_key, **kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    content = s3_hook.read_key(key, bucket_name=bucket_name)
    kwargs['ti'].xcom_push(key=output_key, value=content)
    return content

def determine_next_move(board_state):
    board = chess.Board(board_state)
    with chess.engine.SimpleEngine.popen_uci("/usr/games/stockfish") as engine:
        result = engine.play(board, chess.engine.Limit(time=2.0))
    board.push(result.move)
    return board.fen()

def update_board_state(bucket_name, key, new_board_state, **kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bracket_json = json.loads(s3_hook.read_key(key, bucket_name=bucket_name))
    for round in bracket_json.values():
        for match in round:
            if match["game_status"] == "in_progress":
                match["board_state"].append(new_board_state)
    s3_hook.load_string(json.dumps(bracket_json), key, bucket_name=bucket_name, replace=True)

def process_chess_match(bucket_name, key, **kwargs):
    ti = kwargs['ti']
    bracket_json = ti.xcom_pull(task_ids='retrieve_bracket_json_task', key='bracket_json')
    for round in bracket_json.values():
        for match in round:
            if match["game_status"] == "in_progress":
                current_board_state = match["board_state"][-1]
                new_board_state = determine_next_move(current_board_state)
                update_board_state(bucket_name, key, new_board_state)
                break

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 17),
}

with DAG(
    dag_id='chess_tournament_next_move_peter',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    retrieve_bracket_json_task = PythonOperator(
        task_id='retrieve_bracket_json_task',
        python_callable=retrieve_json_from_s3,
        op_kwargs={
            'bucket_name': 'airflow-chess',
            'key': 'bracket.json',
            'output_key': 'bracket_json'
        },
        provide_context=True
    )

    process_next_move = PythonOperator(
        task_id='process_next_move',
        python_callable=process_chess_match,
        op_kwargs={
            'bucket_name': 'airflow-chess',
            'key': 'bracket.json'
        },
        provide_context=True
    )

    retrieve_bracket_json_task >> process_next_move