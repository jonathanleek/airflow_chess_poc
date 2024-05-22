import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def update_board_state(aws_conn_id, bucket_name, key, new_board_state, **kwargs):
    s3_hook = S3Hook(aws_conn_id= aws_conn_id)
    bracket_json = json.loads(s3_hook.read_key(key, bucket_name=bucket_name))

    # Update the board state in the bracket.json
    for round in bracket_json.values():
        for match in round:
            if match["game_status"] == "in_progress":
                match["board_state"].append(new_board_state)

    s3_hook.load_string(json.dumps(bracket_json), key, bucket_name=bucket_name, replace=True)