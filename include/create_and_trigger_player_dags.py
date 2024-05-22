import json
import os
import subprocess
from airflow.models import DagBag, DagRun
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def create_and_trigger_player_dags(bucket_name, key, repo_name, **kwargs):
    ti = kwargs['ti']
    matches = ti.xcom_pull(task_ids='process_to_be_played_matches')
    github_repo_url = f"https://github.com/jonathanleek/{repo_name}"
    dag_bag = DagBag()

    for match in matches:
        round_key = match["round"]
        match_key = match["match"]
        team1 = match["team1"]
        team2 = match["team2"]

        for team in [team1, team2]:
            branch_name = f"{team}_branch"
            player_dag_id = f"chess_tournament_next_move_{team}"
            local_repo_path = f"/tmp/{team}_repo"

            # Clone the player's branch from GitHub
            if os.path.exists(local_repo_path):
                subprocess.run(["rm", "-rf", local_repo_path], check=True)

            subprocess.run(["git", "clone", "--branch", branch_name, github_repo_url, local_repo_path], check=True)

            # Load the player DAG
            dag_bag.process_file(f"{local_repo_path}/dags/{player_dag_id}.py")
            player_dag = dag_bag.get_dag(player_dag_id)

            if player_dag:
                run_id = f"{round_key}_{match_key}_{team}"
                dr = DagRun(dag_id=player_dag_id, run_id=run_id,
                            conf={"round": round_key, "match": match_key, "player": team})
                dr.verify_integrity()
                dr.run()
