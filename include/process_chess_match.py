from include.determine_next_move import determine_next_move
from include.update_board_state import update_board_state


def process_chess_match(bucket_name, key, **kwargs):
    ti = kwargs['ti']
    # Retrieve the bracket JSON from XCom
    bracket_json = ti.xcom_pull(task_ids='retrieve_bracket_json_task', key='bracket_json')

    # Find the match that is in progress
    for round in bracket_json.values():
        for match in round:
            if match["game_status"] == "in_progress":
                current_board_state = match["board_state"][-1]
                new_board_state = determine_next_move(current_board_state)
                update_board_state(bucket_name, key, new_board_state)
                break
