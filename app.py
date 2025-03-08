import math
import os
import pickle
import uuid
from datetime import datetime
from statistics import mean

import dash
import pandas as pd
import plotly.graph_objs as go
import plotly.io as pio
from dash import dcc, html
from dash.dependencies import Input, Output
from flask import Flask, jsonify, render_template, request, session

from flask_session import Session as FlaskSession
from plotter import (
    plot_apds,
    plot_avg_best_lap,
    plot_best_laps,
    plot_laps_in_session,
    plot_pace_evaluation,
    plot_race,
    plot_valid_invalid_analysis,
)
from queries import *

app = Flask(__name__)
app.config["SESSION_TYPE"] = "filesystem"
FlaskSession(app)

# Initialize Dash app
dash_app = dash.Dash(__name__, server=app, url_base_pathname="/home/")

# Layout for Dash app
dash_app.layout = html.Div()


def check_cache(path: str):
    if os.path.exists(path):
        with open(path, "rb") as f:
            print(f"Loading cache from {path}...", end="")
            pkl = pickle.load(f)
            print("done")
            return pkl

    return None


def save_cache(path: str, data):
    with open(path, "wb") as f:
        pickle.dump(data, f)


@app.route("/")
def home():
    return render_template("home.html")


@app.route("/clear_cache", methods=["POST"])
def clear_cache():
    for file in os.listdir("./cache"):
        os.remove(os.path.join("./cache", file))

    return jsonify({"status": "success", "message": "Cache cleared"})


@app.route("/race_viewer")
def race_viewer():
    return render_template("race_viewer.html")


@app.route("/view_race", methods=["GET"])
def view_race():
    season_number = int(request.args.get("season_number"))
    track_name = request.args.get("track_name")
    division_number = int(request.args.get("division_number"))
    is_practice_race = request.args.get("is_practice_race") == "yes"

    cache_file = (
        f"./cache/ts_weekend_{season_number}_{track_name}_{division_number}.pkl"
    )
    ts_weekend = check_cache(cache_file)
    if not ts_weekend:
        # Generate the plot using the form data
        neo_driver, neo_session = Neo4jDatabase.connect_database("SRA")
        ts_weekend = get_team_series_weekend_by_attrs(
            neo_driver=neo_driver,
            season=season_number,
            track_name=track_name,
            division=division_number,
        )
        ts_weekend.set_drivers(neo_driver=neo_driver)
        Neo4jDatabase.close_connection(neo_driver, neo_session)
        save_cache(cache_file, ts_weekend)

    leader_gaps_fig = plot_race(ts_weekend)
    lap_times_fig = plot_laps_in_session(ts_weekend.ts_race.session)
    # pace_eval_by_finish_fig = plot_pace_evaluation(
    #     ts_weekend.ts_race.session.drivers, by_finish_position=True
    # )
    # pace_eval_by_pits_fig = plot_pace_evaluation(
    #     ts_weekend.ts_race.session.drivers, by_finish_position=False, by_pit_lap=True
    # )

    # Save the plot as an HTML file
    leader_gaps_fig.update_layout(height=800)  # Increase the figure height
    lap_times_fig.update_layout(height=800)  # Increase the figure height
    # pace_eval_by_finish_fig.update_layout(height=800)
    # pace_eval_by_pits_fig.update_layout(height=800)
    leader_gaps_fig_html = pio.to_html(leader_gaps_fig, full_html=False)
    lap_times_fig_html = pio.to_html(lap_times_fig, full_html=False)
    # pace_eval_by_finish_fig_html = pio.to_html(pace_eval_by_finish_fig, full_html=False)
    # pace_eval_by_pits_fig_html = pio.to_html(pace_eval_by_pits_fig, full_html=False)

    return render_template(
        "race_viewer.html",
        leader_gaps_fig_html=leader_gaps_fig_html,
        lap_times_fig_html=lap_times_fig_html,
        # pace_eval_by_finish_fig_html=pace_eval_by_finish_fig_html,
        # pace_eval_by_pits_fig_html=pace_eval_by_pits_fig_html,
        season_number=season_number,
        track_name=track_name,
        division_number=division_number,
    )


@app.route("/track_time_viewer")
def track_time_viewer():
    default_date = (datetime.now() - timedelta(weeks=2)).strftime("%Y-%m-%d")
    return render_template("track_time_viewer.html", default_date=default_date)


@app.route("/view_track_times", methods=["GET"])
def view_track_times():
    track_name = request.args.get("track_name")
    after_date_str = request.args.get("after_date")
    after_date = datetime.strptime(after_date_str, "%Y-%m-%d")
    divs_to_show = set(
        d for d in range(1, 8) if request.args.get(f"show_div_{d}") == "yes"
    )
    do_sort_by_division = request.args.get("sort_by_division") == "yes"

    cache_file = f"./cache/sessions_{track_name}_{after_date_str}.pkl"
    sessions = check_cache(cache_file)
    if not sessions:
        # Generate the plot using the form data
        neo_driver, neo_session = Neo4jDatabase.connect_database("SRA")
        sessions = get_complete_sessions_for_track(
            neo_driver=neo_driver,
            track_name=track_name,
            after_date=after_date,
        )
        Neo4jDatabase.close_connection(neo_driver, neo_session)
        save_cache(cache_file, sessions)

    # Save the plot as an HTML file
    plot_htmls = []
    fig = plot_avg_best_lap(
        sessions=sessions,
        divs_to_show=divs_to_show,
        do_sort_by_division=do_sort_by_division,
        top_n_laps=5,
    )
    fig.update_layout(height=800)
    plot_html = pio.to_html(fig, full_html=False)
    plot_htmls.append(plot_html)

    for attr in ["lap_time", "split1", "split2", "split3"]:
        fig = plot_best_laps(
            sessions,
            lap_attr=attr,
            divs_to_show=divs_to_show,
            do_sort_by_division=do_sort_by_division,
        )
        fig.update_layout(height=800)
        plot_html = pio.to_html(fig, full_html=False)
        with open(f"./cache/{track_name}_{attr}.html", "w", encoding="utf-8") as f:
            f.write(plot_html)
        plot_htmls.append(plot_html)

    fig = plot_valid_invalid_analysis(
        sessions,
        divs_to_show=divs_to_show,
        do_sort_by_division=do_sort_by_division,
    )
    fig.update_layout(height=800)
    plot_html = pio.to_html(fig, full_html=False)
    plot_htmls.append(plot_html)

    for i, plot_html in enumerate(plot_htmls):
        with open(f"./cache/track_times_plot_{i}.html", "w", encoding="utf-8") as f:
            f.write(plot_html)

    return render_template(
        "track_time_viewer.html",
        plot_htmls=plot_htmls,
        track_name=track_name,
        after_date=after_date_str,
        show_divs=divs_to_show,
        sort_by_division=do_sort_by_division,
    )


@app.route("/apd_viewer", methods=["GET"])
def apd_viewer():
    divs_to_show = set(
        d for d in range(1, 8) if request.args.get(f"show_div_{d}") == "yes"
    )

    cache_file = "./cache/sra_drivers.pkl"

    sra_drivers = check_cache(cache_file)
    if not sra_drivers:
        neo_driver, neo_session = Neo4jDatabase.connect_database("SRA")
        sra_drivers = get_sra_drivers(
            neo_driver,
            session_types={"R"},
            # after_date=datetime(2025, 1, 7).replace(tzinfo=pytz_timezone("UTC")),
        )
        Neo4jDatabase.close_connection(neo_driver, neo_session)
        save_cache(cache_file, sra_drivers)

    plot_htmls = []

    # Save the plot as an HTML file
    fig = plot_apds(sra_drivers, divs_to_show, last_n_sessions=5)
    fig.update_layout(height=800)
    plot_htmls.append(pio.to_html(fig, full_html=False))
    plot_htmls.append(
        pio.to_html(
            plot_apds(
                sra_drivers, divs_to_show, last_n_sessions=5, sort_by_division=False
            ).update_layout(height=800),
            full_html=False,
        )
    )

    return render_template("apd_viewer.html", plot_htmls=plot_htmls)


def get_apd_table(max_sessions=5, difference_to_division_cutoff=0.02):

    # get sra_drivers: list[SRADriver]
    # sort by SRADriver.race_division asc, difference between division's ts_avg_percent_diff and driver's ts_avg_percent_diff asc
    # return table `race_division, division, driver name, ts_avg_percent_diff, difference to division`

    cache_file = "./cache/sra_drivers.pkl"
    sra_drivers: list[SRADriver] = check_cache(cache_file)
    if not sra_drivers:
        neo_driver, neo_session = Neo4jDatabase.connect_database("SRA")
        sra_drivers = get_sra_drivers(neo_driver)
        Neo4jDatabase.close_connection(neo_driver, neo_session)
        save_cache(cache_file, sra_drivers)

    div_ts_apds: dict[int, list[float]] = {}
    i = len(sra_drivers) - 1
    while i >= 0:
        driver = sra_drivers[i]
        i -= 1

        if not driver.race_division:
            continue

        driver.avg_ts_apd = driver.cal_avg_ts_apd(
            min_sessions=math.ceil(0.75 * max_sessions),
            max_sessions=max_sessions,
            after_date=datetime(2022, 1, 1).replace(tzinfo=pytz_timezone("UTC")),
        )
        if not driver.avg_ts_apd:
            sra_drivers.pop(i + 1)
            continue

        if driver.race_division not in div_ts_apds:
            div_ts_apds[driver.race_division] = []

        div_ts_apds[driver.race_division].append(driver.avg_ts_apd)

    for div in div_ts_apds:
        div_ts_apds[div] = mean(div_ts_apds[div])

    # Create a DataFrame from sra_drivers
    data = [
        {
            "race_division": driver.race_division,
            "driver_name": driver.name,
            "ts_avg_percent_diff": driver.avg_ts_apd,
            "division_avg_ts_apd": div_ts_apds.get(driver.race_division, float("inf")),
        }
        for driver in sra_drivers
        if driver.race_division
    ]
    df = pd.DataFrame(data)
    # Calculate the difference to division
    df["difference_to_division"] = df["ts_avg_percent_diff"] - df["division_avg_ts_apd"]

    # Filter the DataFrame for differences less than the cutoff
    df = df[df["difference_to_division"] < difference_to_division_cutoff]

    # Sort the DataFrame
    df = df.sort_values(by=["race_division", "difference_to_division"])

    # Format the difference_to_division to avoid scientific notation
    df["difference_to_division"] = df["difference_to_division"].apply(
        lambda x: f"{x:.6f}"
    )

    df = df[
        [
            "race_division",
            "driver_name",
            "difference_to_division",
            "ts_avg_percent_diff",
            "division_avg_ts_apd",
        ]
    ]

    apd_table = df.to_html(index=False)
    return apd_table


@app.route("/insights", methods=["GET"])
def insights():
    max_sessions_for_apd_table = int(request.args.get("max_sessions", 5))
    difference_to_division_cutoff = float(
        request.args.get("difference_to_division_cutoff", 0.02)
    )

    apd_table = get_apd_table(
        max_sessions=max_sessions_for_apd_table,
        difference_to_division_cutoff=difference_to_division_cutoff,
    )

    return render_template(
        "insights.html",
        apd_table=apd_table,
        max_sessions_for_apd_table=max_sessions_for_apd_table,
        difference_to_division_cutoff=difference_to_division_cutoff,
    )


if __name__ == "__main__":
    app.run(debug=True)

# Allow the Dash app to be accessible on the local network
# dash_app.run_server(host="0.0.0.0", port=8050, debug=True)
