import os
import sys

from neo4j import Session

current_dir = os.path.dirname(__file__)
sys.path.append(os.path.join(current_dir, ".."))
from utils.Database import Neo4jDatabase


class SESSION_TYPES:
    RACE = "R"
    QUALIFYING = "Q"


def process_sra_db_neo(neo_session: Session, session_types: list[str]):
    current_dir = os.path.dirname(__file__)
    cyphers_dir = os.path.join(current_dir, "cyphers")
    races_dir = os.path.join(cyphers_dir, "races")
    qualifyings_dir = os.path.join(cyphers_dir, "qualifyings")

    queries = [
        # applies to all ts sessions
        (
            "Creating quali to race relationships...",
            os.path.join(cyphers_dir, "quali_to_race.cypher"),
        ),
        (
            "Assigning division and season numbers to team series sessions...",
            os.path.join(cyphers_dir, "identify_ts_sessions.cypher"),
        ),
        (
            "Creating team series weekend nodes...",
            os.path.join(cyphers_dir, "create_ts_weekends.cypher"),
        ),
        (
            "Creating team series round nodes...",
            os.path.join(cyphers_dir, "create_ts_rounds.cypher"),
        ),
        (
            "Creating team series season nodes...",
            os.path.join(cyphers_dir, "create_ts_seasons.cypher"),
        ),
    ]

    if SESSION_TYPES.RACE in session_types:
        queries.extend(
            [
                (
                    "Calculating average field lap per team series race...",
                    os.path.join(
                        cyphers_dir,
                        races_dir,
                        "set_ts_div_avg_middle_half_lap.cypher",
                    ),
                ),
                (
                    "Calculating average percent difference for team series divisions for races...",
                    os.path.join(
                        cyphers_dir,
                        races_dir,
                        "set_ts_div_avg_percent_diff.cypher",
                    ),
                ),
                (
                    "Calculating average field lap per car per race...",
                    os.path.join(
                        cyphers_dir,
                        races_dir,
                        "set_car_avg_middle_half_lap.cypher",
                    ),
                ),
                (
                    "Calculating average percent difference for cars for races...",
                    os.path.join(
                        cyphers_dir,
                        races_dir,
                        "set_car_avg_percent_diff.cypher",
                    ),
                ),
                (
                    "Calculating team series average percent difference for races...",
                    os.path.join(
                        cyphers_dir,
                        races_dir,
                        "set_car_ts_avg_percent_diff.cypher",
                    ),
                ),
            ]
        )
    if SESSION_TYPES.QUALIFYING in session_types:
        queries.extend(
            [
                (
                    "Calculating average field pot best per team series qualifying...",
                    os.path.join(
                        cyphers_dir,
                        qualifyings_dir,
                        "set_ts_div_avg_middle_half_pot_best.cypher",
                    ),
                ),
                (
                    "Calculating average percent difference for team series divisions for qualifyings...",
                    os.path.join(
                        cyphers_dir,
                        qualifyings_dir,
                        "set_ts_div_avg_percent_diff.cypher",
                    ),
                ),
                (
                    "Calculating average percent difference for cars for qualifyings...",
                    os.path.join(
                        cyphers_dir,
                        qualifyings_dir,
                        "set_car_avg_percent_diff.cypher",
                    ),
                ),
                (
                    "Calculating team series average percent difference for qualifyings...",
                    os.path.join(
                        cyphers_dir,
                        qualifyings_dir,
                        "set_car_ts_avg_percent_diff.cypher",
                    ),
                ),
            ]
        )

    for message, path in queries:
        with open(path, "r") as f:
            query = f.read()
        print(message, end="")
        neo_session.run(query)
        print("done")


if __name__ == "__main__":
    neo_driver, neo_session = Neo4jDatabase.connect_database("SRA")
    process_sra_db_neo(
        neo_session,
        [SESSION_TYPES.QUALIFYING, SESSION_TYPES.RACE],
    )
    Neo4jDatabase.close_connection(neo_driver, neo_session)
