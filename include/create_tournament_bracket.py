import json
import math
import random
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def create_tournament_bracket(participants_json):
    # Parse the input JSON
    participants_data = json.loads(participants_json)
    participants = participants_data['participants']

    # Extract participant names
    participant_names = [p['participant_name'] for p in participants]

    # Shuffle participants for random matchups
    random.shuffle(participant_names)

    # Calculate the number of rounds needed
    num_participants = len(participant_names)
    num_rounds = math.ceil(math.log2(num_participants))
    num_matches = 2 ** num_rounds

    # Add byes if necessary to make the number of participants a power of 2
    while len(participant_names) < num_matches:
        participant_names.append("BYE")

    # Create matchups for the first round
    round1 = []
    for i in range(0, num_matches, 2):
        match = {
            "match": f"match{i // 2 + 1}",
            "team1": participant_names[i],
            "team2": participant_names[i + 1],
            "game_status": "to_be_played",
            "board_state": []
        }
        round1.append(match)

    # Organize the matches into rounds
    bracket = {"round1": round1}

    for round_num in range(2, num_rounds + 1):
        num_matches_in_round = num_matches // (2 ** (round_num - 1))
        round_matches = []
        for i in range(num_matches_in_round):
            match = {
                "match": f"match{i + 1}",
                "team1": None,
                "team2": None,
                "game_status": "to_be_played",
                "board_state": []
            }
            round_matches.append(match)
        bracket[f"round{round_num}"] = round_matches

    # Convert the bracket to JSON
    bracket_json = json.dumps(bracket, indent=4)
    return bracket_json

def create_and_upload_tournament_bracket(participants_json, bucket_name, **kwargs):
    # Generate Bracket
    bracket_json = create_tournament_bracket(participants_json)

    # Upload to S3
    s3_hook = S3Hook()
    s3_hook.load_string(
        string_data = bracket_json,
        bucket_name = bucket_name,
        key = 'bracket.json',
        replace=True
    )