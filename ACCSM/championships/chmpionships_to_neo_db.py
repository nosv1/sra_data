import os
import sys

from neo4j import Session as Neo4jSession

sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))
import json

from championship import *

from Database import Neo4jDatabase

# load championships json
# per championship per race per driver create points node, connect driver and relevant team series node (TeamSeriesRound or TeamSeriesSeason)


def championship_to_neo_db(championship: Championship, neo_session: Neo4jSession):
    neo_team_series_points = []

    # connects drivers to their points in each round
    for driver in championship.driver_standings.drivers:
        for team in driver.teams.values():
            for event_id, event_result in team.event_results.items():
                neo_team_series_points.append(
                    {
                        "key_": f"{championship.season}_{event_id}_{driver.driver_id}",
                        "driver_id": driver.driver_id,
                        "points": event_result.points,
                        "season": championship.season,
                        "track_name": championship.events[event_id]
                        .track_name.lower()
                        .replace(" ", "_"),
                    }
                )
    query = """
        UNWIND $team_series_points as ts_points
        MERGE (tsp:TeamSeriesPoints {
            key_: ts_points.key_
        })
        ON CREATE SET
            tsp.driver_id = ts_points.driver_id,
            tsp.points = ts_points.points
        WITH tsp, ts_points

        MATCH (tsr:TeamSeriesRound {season: ts_points.season, track_name: ts_points.track_name})
        MERGE (tsp)-[:POINTS_FOR]->(tsr)
        WITH tsp, ts_points

        MATCH (d:Driver {driver_id: ts_points.driver_id})
        MERGE (d)-[:POINTS]->(tsp)
    """

    print(f"Creating TeamSeriesPoints nodes for {championship}...", end="")
    neo_session.run(query, parameters={"team_series_points": neo_team_series_points})
    print(f"done")

    # connects points reference to each team series season node
    points_reference = []
    for pos, points in championship.points_reference.points.items():
        points_reference.append(
            {
                "key_": f"{championship.season}_{pos}",
                "position": pos,
                "points": points,
                "season": championship.season,
            }
        )

    query = """
        UNWIND $points_reference as pr
        MERGE (tspr:TeamSeriesPointsReference {
            key_: pr.key_
        })
        SET
            tspr.position = pr.position,
            tspr.points = pr.points,
            tspr.season = pr.season
        WITH tspr, pr

        MATCH (tss:TeamSeriesSeason {season: pr.season})
        MERGE (tspr)-[:POINTS_REFERENCE]->(tss)
    """

    print(f"Creating TeamSeriesPointsReference nodes for {championship}...", end="")
    neo_session.run(query, parameters={"points_reference": points_reference})
    print(f"done")


if __name__ == "__main__":
    current_dir = os.path.dirname(__file__)
    championships_path = os.path.join(current_dir, "championships.json")

    print(f"Loading championships from {championships_path}...", end="")
    championships_json = json.load(open(championships_path, "r"))
    print(f"done")

    try:
        print(f"Creating Championships...", end="")
        championships = [
            Championship.from_json(championship) for championship in championships_json
        ]
        print(f"done")

        neo_driver, neo_session = Neo4jDatabase().connect_database("SRA")

        for championship in championships:
            if not championship:
                continue
            print(f"Creating TeamSeriesPoints nodes for {championship.season}...")
            championship_to_neo_db(championship, neo_session)
            print("done")

    finally:
        Neo4jDatabase.close_connection(neo_driver, neo_session)
