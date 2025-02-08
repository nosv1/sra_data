from __future__ import annotations

import os
import pickle
import statistics
from datetime import datetime, timedelta

import plotly.graph_objects as go

from app import check_cache
from Database import Neo4jDatabase
from plotter import *
from queries import *

# get all races in season
# get drivers from races
# re-assign divs based on current race
# get all sessions in race week
# determine potential bests
# sort drivers per div
# avg middle 50%
# avg top 10% of div 1 as alien
# calculate percent difference from alien
# avg percent differences over entire season per div


if __name__ == "__main__":
    seasons = [11, 12, 13]

    neo_driver, neo_session = Neo4jDatabase.connect_database(db_name="SRA")

    percent_differences_by_season: dict[int, dict[int, dict[str, float]]] = (
        {}
    )  # season: div: track: avg
    for season in seasons:
        # get all races in season
        cache_file = f"cache/team_series_sessions_{season}.pkl"
        ts_sessions: list[TeamSeriesSession] = check_cache(cache_file)
        if False or not ts_sessions:
            ts_sessions = get_team_series_sessions_by_attrs(
                neo_driver,
                seasons={season},
                session_types=["R"],
                has_avg_percent_diff=True,
            )
            for ts_session in ts_sessions:
                # set drivers
                ts_session.session.set_session_drivers(neo_driver)
                # re-assign div
                for driver in ts_session.session.drivers:
                    driver.division = ts_session.division
            ts_sessions = sorted(
                ts_sessions,
                key=lambda tss: tss.session.finish_time.date(),
            )
            pickle.dump(ts_sessions, open(cache_file, "wb"))
        pass

        pot_best_valid_times_by_track_cache_file = (
            f"cache/pot_best_valid_times_by_track_{season}.pkl"
        )
        percent_differences_by_div_cache_file = (
            f"cache/percent_differences_by_div_{season}.pkl"
        )

        # track: div: times
        pot_best_valid_times_by_track: dict[str, dict[int, list[int]]] = check_cache(
            pot_best_valid_times_by_track_cache_file
        )
        # div: track: avg
        percent_differences_by_div: dict[int, dict[str, float]] = {}

        if True or not pot_best_valid_times_by_track:
            pot_best_valid_times_by_track = {}

            # loop through the sessions
            after_date = (ts_sessions[0].session.finish_time - timedelta(days=7)).date()
            track_name = ts_sessions[0].session.track_name
            drivers_divs: dict[str, int] = {}  # driver_id: division
            for tss_idx, ts_session in enumerate(ts_sessions):
                if ts_session.session.track_name == track_name:
                    drivers_divs.update(
                        {d.driver_id: d.division for d in ts_session.session.drivers}
                    )
                    before_date = (
                        ts_session.session.finish_time + timedelta(days=1)
                    ).date()

                    if tss_idx != len(ts_sessions) - 1:
                        continue

                    else:
                        before_date = (
                            ts_sessions[tss_idx - 1].session.finish_time
                            + timedelta(days=1)
                        ).date()

                # get all sessions for current week
                cache_file = (
                    f"cache/sessions_{track_name}_{after_date}_{before_date}.pkl"
                )
                sessions: list[Session] = check_cache(cache_file)
                if False or not sessions:
                    sessions = get_complete_sessions_for_track(
                        neo_driver=neo_driver,
                        track_name=track_name,
                        session_types={"FP"},
                        after_date=after_date,
                        before_date=before_date,
                    )
                    pickle.dump(sessions, open(cache_file, "wb"))
                pass

                # get all drivers for current week -- career drivers have an attr for potential bests
                cache_file = (
                    f"cache/career_drivers_{track_name}_{after_date}_{before_date}.pkl"
                )
                career_drivers_by_div = check_cache(cache_file)
                if False or not career_drivers_by_div:
                    career_drivers: list[CareerDriver] = CareerDriver.from_sessions(
                        sessions
                    )
                    career_drivers_by_div: dict[int, list[CareerDriver]] = {}
                    for cd in career_drivers:
                        # driver was not in race
                        if cd.driver.driver_id not in drivers_divs:
                            continue
                        # driver did not set a valid lap
                        if not cd.potential_best_valid_lap_time:
                            continue
                        # if driver is not in gt3 car
                        if cd.driver.car.car_group != "GT3":
                            continue
                        cd.driver.division = drivers_divs[cd.driver.driver_id]
                        if cd.driver.division not in career_drivers_by_div:
                            career_drivers_by_div[cd.driver.division] = []
                        career_drivers_by_div[cd.driver.division].append(cd)

                    # sort drivers per div
                    for div, cds in career_drivers_by_div.items():
                        career_drivers_by_div[div] = sorted(
                            cds, key=lambda cd: cd.potential_best_valid_lap_time
                        )
                    pickle.dump(career_drivers_by_div, open(cache_file, "wb"))
                pass

                # middle 50%
                pot_best_valid_times_by_div: dict[int, list[int]] = {
                    div: [
                        cd.potential_best_valid_lap_time / 1000.0
                        for cd in career_drivers_by_div[div][
                            len(career_drivers_by_div[div])
                            // 4 : 3
                            * len(career_drivers_by_div[div])
                            // 4
                        ]
                    ]
                    for div in career_drivers_by_div
                }
                # top 10% of div 1 as alien
                pot_best_valid_times_by_div.update(
                    {
                        0: [
                            cd.potential_best_valid_lap_time / 1000.0
                            for cd in career_drivers_by_div[1][
                                : len(career_drivers_by_div[1]) // 10
                            ]
                        ]
                    }
                )
                for div, times in pot_best_valid_times_by_div.items():
                    if track_name not in pot_best_valid_times_by_track:
                        pot_best_valid_times_by_track[track_name] = {}
                    pot_best_valid_times_by_track[track_name][div] = times

                # reset for new week
                after_date = before_date
                track_name = ts_session.session.track_name
                drivers_divs = {
                    d.driver_id: d.division for d in ts_session.session.drivers
                }
                pass

            pickle.dump(
                pot_best_valid_times_by_track,
                open(pot_best_valid_times_by_track_cache_file, "wb"),
            )

        # Create a figure to plot the best times per track
        for track, div_times in pot_best_valid_times_by_track.items():
            fig = go.Figure()
            fig.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",  # Set the plot background color to transparent
                paper_bgcolor="#2c2c2c",  # Set the paper background color to match the site's background color
                font=dict(
                    size=14, color="#e0e0e0"
                ),  # Set the font color to match the site's text color
                hovermode="closest",
                xaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
                yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
            )

            div_times = sorted(div_times.items(), key=lambda x: x[0])
            annotations = []
            for div, times in div_times:
                percent_diff = statistics.mean(times) / statistics.mean(
                    pot_best_valid_times_by_track[track][0]
                )
                if div not in percent_differences_by_div:
                    percent_differences_by_div[div] = {}
                percent_differences_by_div[div][track] = percent_diff

                div_color = SRA_DIV_COLOR.from_division(div)
                times_color = div_color.darken() if div_color.is_bright() else div_color
                avg_color = div_color.darken().darken()
                fig.add_trace(
                    go.Scatter(
                        x=[i for i, _ in enumerate(times)],
                        y=[time for time in times],
                        mode="lines+markers",
                        name=f"Division {div}",
                        marker=dict(size=12),
                        line=dict(color=times_color.to_rgba(), width=2),
                    )
                )
                # Add horizontal line for average time per division
                avg_time = statistics.mean(times)
                fig.add_trace(
                    go.Scatter(
                        x=[0, len(times) - 1],
                        y=[avg_time, avg_time],
                        mode="lines",
                        name=f"Division {div} Avg",
                        line=dict(color=avg_color.to_rgba(), width=2, dash="dash"),
                    )
                )
                annotations.append(
                    dict(
                        x=len(times) - 1,
                        y=avg_time,
                        xref="x",
                        yref="y",
                        text=f"Avg: {avg_time:.2f}s -- {percent_diff:.1%} from alien",
                        showarrow=True,
                        arrowhead=2,
                        ax=50,
                        ay=-30,
                        font=dict(color=avg_color.brighten().to_rgba()),
                        bgcolor=avg_color.to_rgba(),
                    )
                )

            fig.update_layout(
                title=f"The middle 50% of potential best valid lap times per division at {track} for season {season}",
                xaxis_title="Driver",
                yaxis_title="Lap Time (s)",
                showlegend=True,
                annotations=annotations,
            )

            # fig.show()
            fig.update_layout(height=1000, width=2000)
            fig.write_image(f"reference_times/middle_50_{track}_{season}.png")

        percent_differences_by_season[season] = percent_differences_by_div

        # plot the percent diffs per track per div
        fig = go.Figure()
        fig.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",  # Set the plot background color to transparent
            paper_bgcolor="#2c2c2c",  # Set the paper background color to match the site's background color
            font=dict(
                size=14, color="#e0e0e0"
            ),  # Set the font color to match the site's text color
            hovermode="closest",
            xaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
            yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
        )

        annotations = []
        for div, track_diffs in percent_differences_by_div.items():
            track_diffs = sorted(track_diffs.items(), key=lambda x: x[0])
            fig.add_trace(
                go.Scatter(
                    x=[track for track, _ in track_diffs],
                    y=[diff for _, diff in track_diffs],
                    mode="lines+markers",
                    text=[f"{diff:.1%}" for _, diff in track_diffs],
                    name=f"Division {div}",
                    marker=dict(size=12),
                    line=dict(
                        color=SRA_DIV_COLOR.from_division(div).to_rgba(), width=2
                    ),
                )
            )
            for track, diff in track_diffs:
                annotations.append(
                    dict(
                        x=track,
                        y=diff,
                        xref="x",
                        yref="y",
                        text=f"{diff:.1%}",
                        showarrow=True,
                        arrowhead=2,
                        ax=50,
                        ay=-30,
                        font=dict(
                            color=SRA_DIV_COLOR.from_division(div)
                            .darken()
                            .darken()
                            .to_rgba()
                        ),
                        bgcolor=SRA_DIV_COLOR.from_division(div).brighten().to_rgba(),
                    )
                )

        fig.update_layout(
            title=f"Average percent difference from alien per division at each track for season {season}",
            xaxis_title="Track",
            yaxis_title="Avg % Difference from Alien",
            showlegend=True,
            annotations=annotations,
        )

        # fig.show()
        fig.update_layout(height=1000, width=2000)
        fig.write_image(f"reference_times/avg_diffs_per_track_{season}.png")

    Neo4jDatabase.close_connection(neo_driver, neo_session)

    # Create a line plot for each division
    fig = go.Figure()

    annotations = []
    for div in range(0, 7):
        seasons = []
        avg_diffs = []
        for season, track_diffs in percent_differences_by_season.items():
            if div in track_diffs:
                seasons.append(season)
                avg_diffs.append(
                    statistics.mean([diff for _, diff in track_diffs[div].items()])
                )

        fig.add_trace(
            go.Scatter(
                x=seasons,
                y=avg_diffs,
                mode="lines+markers",
                name=f"Division {div}",
                marker=dict(size=8),
                line=dict(width=2, color=SRA_DIV_COLOR.from_division(div).to_rgba()),
            )
        )

        for season, avg_diff in zip(seasons, avg_diffs):
            annotations.append(
                dict(
                    x=season,
                    y=avg_diff,
                    xref="x",
                    yref="y",
                    text=f"{avg_diff:.1%}",
                    showarrow=True,
                    arrowhead=2,
                    ax=50,
                    ay=-30,
                    font=dict(
                        color=SRA_DIV_COLOR.from_division(div)
                        .darken()
                        .darken()
                        .to_rgba()
                    ),
                    bgcolor=SRA_DIV_COLOR.from_division(div).brighten().to_rgba(),
                )
            )

    fig.update_layout(
        title="Average % Difference from Alien per Division Across Seasons",
        xaxis_title="Season",
        yaxis_title="Avg % Difference from Alien",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="#2c2c2c",
        font=dict(size=14, color="#e0e0e0"),
        hovermode="closest",
        xaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
        yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
        annotations=annotations,
    )
    fig.update_layout(height=1000, width=2000)
    fig.write_image("reference_times/avg_diffs_per_div_across_seasons.png")

    # fig.show()
    pass
