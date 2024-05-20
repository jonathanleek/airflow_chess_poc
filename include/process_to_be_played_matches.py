import json
from include.retrieve_json_from_s3 import retrieve_json_from_s3


def process_to_be_played_matches(bucket_name, key, **kwargs):
    # Retrieve the bracket JSON using the retrieve_json_from_s3 function
    bracket_json = retrieve_json_from_s3(bucket_name, key, 'bracket_json', **kwargs)

    # Load the bracket JSON
    bracket = json.loads(bracket_json)

    to_be_played_matches = []

    # Loop through each round
    for round_key, matches in bracket.items():
        # Loop through each match in the round
        for match in matches:
            if match["game_status"] == "to_be_played":
                team1 = match.get("team1")
                team2 = match.get("team2")

                if team1 and team1 != "BYE" and team2 and team2 != "BYE":
                    to_be_played_matches.append({
                        "round": round_key,
                        "match": match["match"],
                        "team1": team1,
                        "team2": team2,
                        "game_status": match["game_status"]
                    })

    return to_be_played_matches