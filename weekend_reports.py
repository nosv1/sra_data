from __future__ import annotations

import os
import pickle
from datetime import datetime

import plotly.graph_objects as go
from pandas import DataFrame

from app import check_cache
from Database import Neo4jDatabase
from plotter import *
from queries import *


def save_fig(fig: go.Figure, path: str):
    with open(path, "w", encoding="utf-8") as f:
        f.write(fig.to_html())


def generate_report(weekend: Weekend):
    current_dir = os.path.dirname(__file__)
    weekend_dir = f"{current_dir}/{weekend.path}"
    if not os.path.exists(weekend_dir):
        os.makedirs(weekend_dir)
    pre = (
        (f"{weekend.race.track_name}_{weekend.race.server_name}")
        .replace("#", "")
        .replace("|", "")
        .replace(".", "")
        .replace(":", "")
        .replace(" ", "_")
    )

    # figs: dict[str, go.Figure] = {
    figs: dict[str, DataFrame] = {
        # "race": plot_race(weekend),
        # "lap_times": plot_laps_in_session(weekend.race),
        "pace_eval_by_finish": plot_pace_evaluation(
            weekend.race.drivers,
            include_lapped_drivers=False,
            by_finish_position=True,
            by_pit_lap=False,
        ),
        "pace_eval_by_pit": plot_pace_evaluation(
            weekend.race.drivers,
            include_lapped_drivers=False,
            by_finish_position=False,
            by_pit_lap=True,
        ),
    }

    figs["pace_eval_by_finish"].to_csv(f"{weekend_dir}/{pre}_pace_eval_by_finish.csv")
    figs["pace_eval_by_pit"].to_csv(f"{weekend_dir}/{pre}_pace_eval_by_pit.csv")

    # for key, fig in figs.items():
    #     fig.update_layout(height=800)
    #     save_fig(fig, f"{weekend_dir}/{pre}_{key}.html")


if __name__ == "__main__":

    dates = [
        # ts week 1
        # datetime(2024, 12, 10),
        # datetime(2024, 12, 11),
        # # ts week 2
        # datetime(2024, 12, 17),
        # datetime(2024, 12, 18),
        # # ts week 3
        # datetime(2024, 1, 7),
        # datetime(2024, 1, 8),
        # ts week 4
        # datetime(2025, 1, 14),
        # datetime(2025, 1, 15),
        # ts week 5
        datetime(2025, 1, 21),
        datetime(2025, 1, 22),
        # # ts week 6
        # datetime(2025, 2, 4),
        # datetime(2025, 2, 5),
        # # ts week 7
        # datetime(2025, 2, 11),
        # datetime(2025, 2, 12),
        # # ts week 8
        # datetime(2025, 2, 18),
        # datetime(2025, 2, 19),
    ]
    for date in dates:
        cache_file = f"./cache/weekends_{date.year}_{date.month}_{date.day}.pkl"

        if False or check_cache(cache_file):
            with open(cache_file, "rb") as f:
                weekends = pickle.load(f)
        else:
            weekends: list[Weekend] = []

            neo_driver, neo_session = Neo4jDatabase.connect_database("SRA")

            for session_key in get_session_keys(
                neo_driver,
                after_date=date,
                before_date=date + timedelta(days=1),
                session_types={"R"},
            ):
                weekend = get_weekend_from_session_key(neo_driver, session_key)
                weekend.set_drivers(neo_driver)

                weekends.append(weekend)

            Neo4jDatabase.close_connection(neo_driver, neo_session)

            with open(cache_file, "wb") as f:
                pickle.dump(weekends, f)

        for weekend in weekends:
            generate_report(weekend)

    pass
