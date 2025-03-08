from __future__ import annotations

import json
import os
import pickle
import statistics
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
from PIL import Image
from PIL.ImageFile import ImageFile
from scipy.stats import linregress

from Database import Neo4jDatabase
from utils.queries import *


class SRA_DIV_COLOR:
    DIV_COLORS = {
        0: (192, 192, 192),  # silver
        1: (255, 255, 255),  # white
        2: (63, 63, 63),  # dark gray
        3: (255, 0, 0),  # red
        4: (127, 0, 127),  # bright purple
        5: (0, 128, 0),  # green
        6: (255, 165, 0),  # orange
    }

    def __init__(self, r, g, b):
        self.r = r
        self.g = g
        self.b = b

    def __iter__(self):
        return iter([self.r, self.g, self.b])

    @staticmethod
    def from_division(division: int) -> SRA_DIV_COLOR:
        return SRA_DIV_COLOR(*SRA_DIV_COLOR.DIV_COLORS[division])

    def to_rgba(self, opacity: float = 1.0):
        return f"rgba({self.r}, {self.g}, {self.b}, {opacity})"

    def apply_silver_tint(self) -> SRA_DIV_COLOR:
        return SRA_DIV_COLOR(*[min((c + 200) // 2, 255) for c in self])

    def is_bright(self, threshold=255) -> bool:
        return sum([self.r, self.g, self.b]) >= threshold

    def darken(self, factor=0.25) -> SRA_DIV_COLOR:
        return SRA_DIV_COLOR(*[min(c - c * factor, 255) for c in self])

    def brighten(self, factor=0.75) -> SRA_DIV_COLOR:
        return SRA_DIV_COLOR(*[max(c + (255 - c) * factor, 0) for c in self])


def plot_apds(
    drivers: list[SRADriver],
    divs_to_show: set[int] = set(),
    sort_by_division: bool = True,
    last_n_sessions: int = 8,
):
    """average percent differences"""
    fig = go.Figure()
    fig.update_layout(
        title="Average Percent Differences",
        xaxis_title="Driver",
        yaxis_title="APD %",
        plot_bgcolor="rgba(0,0,0,0)",  # Set the plot background color to transparent
        paper_bgcolor="#2c2c2c",  # Set the paper background color to match the site's background color
        font=dict(color="#e0e0e0"),  # Set the font color to match the site's text color
        hovermode="closest",
        xaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
        yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
    )
    fig.update_yaxes(autorange="reversed")

    drivers = [
        driver
        for driver in sorted(
            drivers,
            key=lambda d: (
                d.division if d.division else float("inf"),
                d.first_name,
                d.last_name,
            ),
        )
        if driver.division
        and (not divs_to_show or driver.race_division in divs_to_show)
    ]

    apd_drivers: list[tuple[SessionDriver, float, float, list[tuple[Car, Session]]]] = (
        []
    )  # driver, avg_apd, trend
    min_trend = float("inf")
    max_trend = float("-inf")
    for d_idx, driver in enumerate(drivers):
        if not driver.division:
            continue

        filtered_cars_sessions: list[tuple[Car, Session]] = []  # car, session
        cs_idx = len(driver.sessions) - 1
        while cs_idx >= 0 and len(filtered_cars_sessions) < last_n_sessions:
            car: Car = driver.cars[cs_idx]
            session: Session = driver.sessions[cs_idx]
            if car.ts_avg_percent_diff:
                filtered_cars_sessions.append((car, session))
            cs_idx -= 1

        if not filtered_cars_sessions:
            continue

        avg_apd = statistics.mean(
            [c.ts_avg_percent_diff for c, _ in filtered_cars_sessions]
        )
        if len(filtered_cars_sessions) > 1:
            trend = (
                linregress(
                    [i for i in range(len(filtered_cars_sessions))],
                    [c.ts_avg_percent_diff for c, _ in filtered_cars_sessions],
                ).slope
                * -1
            )
            # negating because filtered_cars_sessions is in reverse order
        else:
            trend = 0
        min_trend = min(min_trend, trend)
        max_trend = max(max_trend, trend)

        apd_drivers.append((driver, avg_apd, trend, filtered_cars_sessions))

    apd_drivers = sorted(
        apd_drivers,
        key=lambda d_a_t: (
            d_a_t[0].race_division if sort_by_division else float("inf"),
            d_a_t[1],
        ),
    )

    annotations = []
    data = []

    for d_idx, (driver, avg_apd, trend, filtered_cars_sessions) in enumerate(
        apd_drivers
    ):
        # Darken the division color based on the slope
        division_color = (
            SRA_DIV_COLOR.from_division(driver.race_division).darken().darken().darken()
        )
        division_color = division_color.brighten(
            1 - (trend - min_trend) / (max_trend - min_trend)
        )
        text = (
            f"[ {driver.division} {driver.name} ]<br>"
            f"------------ Stats ------------<br>"
            f"Avg APD ({len(filtered_cars_sessions)}): {avg_apd*100.0:.3f}%<br>"
            f"Slope: {'+' if trend > 0 else ''}{trend*100.0:.3f}%<br>"
            f"------------ Sessions ------------<br>"
            f"{'<br>'.join([f'{c.ts_avg_percent_diff*100.0:.3f}% -- {s.track_name} -- {s.server_name}' for c, s in filtered_cars_sessions])}"
        )
        fig.add_trace(
            go.Bar(
                x=[driver.name],
                y=[avg_apd * 100.0],
                name=f"{driver.division} {driver.name}",
                text=text,
                marker_color=division_color.to_rgba(),
                marker_line=dict(color=division_color.to_rgba()),
                orientation="v",
                width=0.8,
                # textpositionsrc="y",
            )
        )

        if abs(trend - min_trend) / abs(min_trend) <= 0.20:
            annotations.append(
                dict(
                    x=driver.name,
                    y=-avg_apd * 100.0 + (1 if avg_apd < 0 else -1),
                    xref="x",
                    yref="y",
                    text=text,
                    showarrow=True,
                    arrowhead=7,
                    ax=0,
                    ay=-40,
                    bgcolor=SRA_DIV_COLOR.from_division(driver.race_division)
                    .darken()
                    .to_rgba(),
                    bordercolor=division_color.to_rgba(),
                    borderwidth=2,
                )
            )

        data.append(
            {
                "Driver": driver.name,
                "Division": driver.division,
                "Avg APD (%)": avg_apd * 100.0,
                "Trend (%)": trend * 100.0,
                "Sessions": len(filtered_cars_sessions),
            }
        )

    fig.update_layout(annotations=annotations)

    df = pd.DataFrame(data)
    df.to_csv("average_percent_differences.csv", index=False)

    return fig


def plot_total_times(drivers: list[SessionDriver]):
    # driver.car.total_time
    # sum([lt.lap_time for lt in driver.laps])
    fig = go.Figure()
    fig.update_layout(
        title="Total Times",
        xaxis_title="Driver",
        yaxis_title="Total Time (s)",
        plot_bgcolor="rgba(0,0,0,0)",  # Set the plot background color to transparent
        paper_bgcolor="#2c2c2c",  # Set the paper background color to match the site's background color
        font=dict(color="#e0e0e0"),  # Set the font color to match the site's text color
        hovermode="closest",
        xaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
        yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
    )

    drivers = sorted(drivers, key=lambda x: len(x.laps) if x.laps else 0, reverse=True)
    drivers.sort(key=lambda d: d.start_position)
    fig.add_trace(
        go.Bar(
            x=[driver.name for driver in drivers],
            y=[driver.car.total_time if driver.car else 0 for driver in drivers],
            name="Total Time",
            marker=dict(color="rgba(255, 255, 255, 0.5)"),
        )
    )

    fig.add_trace(
        go.Bar(
            x=[driver.name for driver in drivers],
            y=[
                # (driver.sum_splits if driver.car and driver.laps else 0)
                driver.start_offset
                for driver in drivers
            ],
            name="Sum of Lap Times",
            marker=dict(color="rgba(0, 122, 204, 0.5)"),
            text=[
                f"Lap Count: {driver.car.lap_count if driver.car else 0}, Laps: {len(driver.laps) if driver.laps else 0}"
                for driver in drivers
            ],
            textposition="outside",
        )
    )

    fig.update_layout(barmode="group")

    return fig


# def plot_race(ts_weekend: TeamSeriesWeekend) -> go.Figure:
def plot_race(weekend: Weekend) -> go.Figure:
    # plot the total times per driver.car.total_time
    fig = go.Figure()
    fig.update_layout(
        # title=f"Season {ts_weekend.ts_race.season} - Division {ts_weekend.ts_race.division} - {ts_weekend.ts_race.session.track_name.replace('_', ' ').title()} - Drivers: {len(ts_weekend.ts_race.session.drivers)}",
        title=f"{weekend.race.track_name.replace('_', ' ').title()} - Division {weekend.race.drivers[0].race_division} - Drivers: {len(weekend.race.drivers)}",
        xaxis_title="Lap",
        yaxis_title="Gap to Leader (s)",
        # plot_bgcolor="rgba(0,0,0,0)",  # Set the plot background color to transparent
        # paper_bgcolor="rgba(30, 22, 34, 1)",  # Set the paper background color to white
        # font=dict(color="white"),  # Set the font color to white
        plot_bgcolor="rgba(0,0,0,0)",  # Set the plot background color to transparent
        paper_bgcolor="#2c2c2c",  # Set the paper background color to match the site's background color
        font=dict(color="#e0e0e0"),  # Set the font color to match the site's text color
        hovermode="closest",
        xaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
        yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
    )
    fig.add_trace(
        go.Scatter(
            x=[None],
            y=[None],
            mode="markers",
            name="Pit Stops (Probable)",
            marker=dict(color="rgba(255, 255, 255, 0.5)", size=10),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=[None],
            y=[None],
            mode="lines",
            name="Gap to Leader",
            line=dict(color="rgba(255, 255, 255, 0.5)"),
        )
    )

    race_drivers = sorted(weekend.race.drivers, key=lambda d: d.car.finish_position)

    # https://coolors.co/211926-515ca8-f0eef3-c6c1d3-8d7ea0
    starting_colors = [
        (198, 193, 211),  # french gray
        (185, 107, 137),  # thulian pink
        (240, 238, 243),  # ghost white
        (81, 92, 168),  # savoy blue
        (141, 126, 160),  # mountbatten pink
        (193, 185, 203),  # lavender gray
    ]

    starting_colors = [
        (255, 204, 0),  # Highlight color
        (255, 77, 77),  # Button color
        # (255, 51, 51),  # Hover color
        (224, 224, 224),  # Text color
        # (44, 44, 44),  # Background color
        (0, 122, 204),  # blue
        (0, 204, 102),  # green
    ]
    colors = []
    for d_idx, driver in enumerate(race_drivers):
        # starting color
        base_color = starting_colors[d_idx % len(starting_colors)]

        # fade opacity based on how far down the list the driver is
        min_opacity = 0.3
        opacity = (
            1
            - (
                (d_idx // len(starting_colors))
                / (len(race_drivers) / len(starting_colors) - 1)
            )
        ) * min_opacity + min_opacity
        color = f"rgba({base_color[0]}, {base_color[1]}, {base_color[2]}, {opacity})"
        colors.append(color)

    for d_idx, driver in enumerate(race_drivers):
        for srp_idx in range(len(driver.split_running_time)):
            if (
                driver.probable_pit_laps
                and (srp_idx + 2) / 3 in driver.probable_pit_laps
            ):
                fig.add_trace(
                    go.Scatter(
                        x=[srp_idx - 1],
                        y=[driver.gap_to_leader_per_split[srp_idx - 1]],
                        mode="markers",
                        name=driver.name,
                        marker=dict(color=colors[d_idx], size=10),
                    )
                )

        fig.add_trace(
            go.Scatter(
                x=list(range(len(driver.gap_to_leader_per_split))),
                # y=[(g if g >= 0 else 0) for g in driver.gap_to_leader_per_split],
                y=[g for g in driver.gap_to_leader_per_split],
                mode="lines+markers",
                name=driver.name,
                line=dict(color=colors[d_idx]),
            )
        )

    fig.update_yaxes(autorange="reversed")

    fig.update_xaxes(
        tickmode="array",
        tickvals=list(range(len(race_drivers[0].gap_to_leader_per_split))),
        ticktext=[
            f"{i/3 + 1:.1f}"
            for i in range(len(race_drivers[0].gap_to_leader_per_split))
        ],
    )

    return fig


def plot_pace_evaluation(
    drivers: list[SessionDriver],
    include_lapped_drivers=False,
    by_finish_position: bool = True,
    by_pit_lap: bool = False,
) -> pd.DataFrame:
    # ) -> go.Figure:
    # x axis is finish position
    # y axis is gap to leader
    # size of marker is how close you were to others during the race (we'll do this last)
    fig = go.Figure()
    gap_margin = 1.0

    drivers = sorted(
        drivers,
        key=lambda d: d.car.finish_position if d.car else float("inf"),
    )
    fighters_dict: dict[SessionDriver, int] = {}
    # driver, num splits within 2 seconds of leader

    for split_idx in range(len(drivers[0].gap_to_leader_per_split)):
        gaps_to_leader: list[tuple[SessionDriver, float]] = []
        for driver in drivers:
            if not driver.car or (
                not include_lapped_drivers
                and drivers[0].car.lap_count - driver.car.lap_count > 0
            ):
                continue
            if split_idx >= len(driver.gap_to_leader_per_split):
                continue
            gaps_to_leader.append((driver, driver.gap_to_leader_per_split[split_idx]))

        gaps_to_leader.sort(key=lambda d_g: d_g[1])

        for d_idx, (driver, gap_to_leader) in enumerate(gaps_to_leader):
            if split_idx // 3 >= len(driver.laps):
                continue

            for o_idx, (_, opponent_gap_to_leader) in enumerate(gaps_to_leader):
                if o_idx == d_idx:
                    continue

                opponent = gaps_to_leader[o_idx][0]
                if driver.race_division != opponent.race_division:
                    continue

                if driver not in fighters_dict:
                    fighters_dict[driver] = 0

                opponent_gap_to_leader = gaps_to_leader[o_idx][1]
                if abs(gap_to_leader - opponent_gap_to_leader) < gap_margin:
                    fighters_dict[driver] += (
                        driver.laps[split_idx // 3].splits[split_idx % 3] / 1000.0
                    )

                elif o_idx > d_idx:
                    break

    min_duration_fighting = min(d for d in fighters_dict.values())
    max_duration_fighting = max(d for d in fighters_dict.values())

    fighters: list[tuple[SessionDriver, int]] = sorted(
        fighters_dict.items(), key=lambda d_n: d_n[0].car.finish_position
    )

    race_duration = drivers[0].sum_splits / 1000.0
    # fig.add_trace(
    #     go.Scatter(
    #         x=[f"{fighters[0][0].last_name} #{fighters[0][0].car.car_number}"],
    #         y=[fighters[-1][0].gap_to_leader_per_split[-1]],
    #         mode="markers+text",
    #         name=f"Loneliest Driver {min_duration_fighting:.3f}",
    #         text=f"{(min_duration_fighting / race_duration) * 100.0:.0f}%",
    #         marker=dict(
    #             color="rgba(255,255,255,0.1)",
    #             size=20 + 20 * (min_duration_fighting / race_duration),
    #         ),
    #         marker_line=dict(color="rgba(255,255,255,0.1)", width=2),
    #         textposition="middle center",
    #     )
    # )
    # fig.add_trace(
    #     go.Scatter(
    #         x=[f"{fighters[1][0].last_name} #{fighters[1][0].car.car_number}"],
    #         y=[fighters[-1][0].gap_to_leader_per_split[-1]],
    #         mode="markers+text",
    #         name=f"Least Lonely Driver {max_duration_fighting:.3f}",
    #         text=f"{(max_duration_fighting / race_duration) * 100.0:.0f}%",
    #         marker=dict(
    #             color="rgba(255,255,255,0.1)",
    #             size=20 + 20 * (max_duration_fighting / race_duration),
    #         ),
    #         marker_line=dict(color="rgba(255,255,255,0.1)", width=2),
    #         textposition="middle center",
    #     )
    # )

    data = []
    min_lap_time = float("inf")
    max_lap_time = float("-inf")
    min_variance = float("inf")
    max_variance = float("-inf")
    min_color = float("inf")
    for d_idx, (driver, duration_fighting) in enumerate(fighters):
        division_color = SRA_DIV_COLOR.from_division(driver.race_division)
        driver_color = (
            division_color.apply_silver_tint()
            if driver.is_silver_driver
            else division_color
        )
        # division_color = (
        #     division_color.darken() if division_color.is_bright() else division_color
        # )
        # fig.add_trace(
        #     go.Scatter(
        #         x=[f"{driver.last_name} #{driver.car.car_number}"],
        #         y=[driver.gap_to_leader_per_split[-1]],
        #         mode="markers+text",
        #         name=driver.name,
        #         text=f"{(duration_fighting / race_duration) * 100.0:.0f}%",
        #         marker=dict(
        #             color=division_color.to_rgba(),
        #             size=20 + 40 * (duration_fighting / max_duration_fighting),
        #         ),
        #         marker_line=dict(color="rgba(255,255,255,0.1)", width=2),
        #         textposition="middle center",
        #     )
        # )

        # variance and avg pace per stint, and based on stint
        stint_lap_counts: list[int] = []
        stint_avgs: list[float] = []
        stint_variances: list[float] = []
        total_laps = 0
        for stint in driver.stints:
            if not stint.laps:
                continue
            stint_laps = sorted([l.lap_time for l in stint.laps])
            top_75_percent = stint_laps[: len(stint_laps) * 3 // 4]
            if len(top_75_percent) < 3:
                continue
            stint_lap_counts.append(len(top_75_percent))
            stint_avgs.append(statistics.mean(top_75_percent))
            stint_variances.append(statistics.variance(top_75_percent))
            total_laps += len(top_75_percent)

        if not stint_lap_counts:
            continue

        average_lap_time = sum(
            [
                avg * stint_lap_counts[a_i] / total_laps
                for a_i, avg in enumerate(stint_avgs)
            ]
        )
        variance = sum(
            [
                var * stint_lap_counts[v_i] / total_laps
                for v_i, var in enumerate(stint_variances)
            ]
        )

        # variance and avg pace based on middle 50% of all laps
        # driver_laps = sorted(
        #     [
        #         l
        #         for l in driver.laps
        #         if (
        #             l.lap_number not in driver.probable_pit_laps
        #             and l.lap_number + 1 not in driver.probable_pit_laps
        #             and l.lap_number > 1
        #         )
        #     ],
        #     key=lambda l: l.lap_time,
        # )
        # middle_50_percent = driver_laps[
        #     len(driver_laps) // 4 : len(driver_laps) * 3 // 4
        # ]
        # if len(middle_50_percent) < 3:
        #     continue

        # average_lap_time = statistics.mean([l.lap_time for l in middle_50_percent])
        # variance = statistics.variance([l.lap_time for l in middle_50_percent])

        min_lap_time = min(min_lap_time, average_lap_time)
        max_lap_time = max(max_lap_time, average_lap_time)
        min_variance = min(min_variance, variance)
        max_variance = max(max_variance, variance)
        color = (
            "FinishPosition"
            if by_finish_position
            else "PitLap" if by_pit_lap else "Color"
        )

        car_file_path = driver.car.model.get_logo_path(
            current_dir=os.path.dirname(__file__)
        )
        data.append(
            {
                "Driver": driver.name,
                "RaceDivision": driver.race_division,
                "AverageLapTime": average_lap_time / 1000.0,
                "Variance": variance / 1000.0,
                "FinishPosition": driver.car.finish_position,
                # "PitLap": driver.probable_pit_laps[0] if by_pit_lap else 0,
                "PitLap": (
                    driver.probable_pit_laps[0]
                    if driver.probable_pit_laps
                    else float("inf")
                ),
                "CarNumber": driver.car.car_number,
                "Text": f"{driver.last_name} #{driver.car.car_number}",
                "BattlePercent": (duration_fighting / race_duration),
                "Size": 0 + 40 * (duration_fighting / race_duration),
                "Marker": dict(line=dict(color=division_color.to_rgba(), width=2)),
                "FontColor": (
                    driver_color
                    if division_color.is_bright()
                    else driver_color.brighten()
                ).to_rgba(),
            }
        )
        min_color = min(min_color, data[-1][color])

    data.append(
        {
            "AverageLapTime": min_lap_time / 1000.0,
            "Variance": max_variance / 1000.0,
            color: min_color,
            "Text": "",
            "Size": 0 + 40 * (max_duration_fighting / race_duration),
        }
    )
    data.append(
        {
            "AverageLapTime": min_lap_time / 1000.0,
            "Variance": max_variance / 1000.0,
            color: min_color,
            "Text": f"Max: {(max_duration_fighting / race_duration) * 100.0:.0f}%<br>"
            f"Min: {(min_duration_fighting / race_duration) * 100.0:.0f}%",
            "Size": 0 + 40 * (min_duration_fighting / race_duration),
        }
    )

    for row in data:
        row["PercentDiff"] = row["AverageLapTime"] / data[-1]["AverageLapTime"]
        # kinda gross assuming last row is min lap time

    df = pd.DataFrame(data)
    return df
    fig = px.scatter(
        df,
        x="PercentDiff",
        y="Variance",
        # text="Text",
        color=color,
        hover_data={"Driver": True, "CarNumber": True},
        color_continuous_scale="Viridis",
        labels={"AverageLapTime": "Average Lap Time (s)", "Variance": "Variance (s)"},
        size="Size",
        size_max=40,
    )
    annotations = []
    for _, row in df.iterrows():
        annotation_dict = dict(
            x=row["PercentDiff"],
            y=row["Variance"],
            xref="x",
            yref="y",
            text=row["Text"],
            textangle=-45,
            showarrow=False,
        )
        if pd.notna(row["FontColor"]):
            annotation_dict["font"] = dict(color=row["FontColor"])
        annotations.append(annotation_dict)

    fig.update_layout(annotations=annotations)

    fig.update_layout(
        title=f"Race Pace Evaluation<br>"
        f"<sup>- The variance and average are based on the top 75% of a driver's laps in each of their stints, where each stint is weighted based on the length of the stint. Before cutting to top 75%, all laps are included except for lap one and pit-affected laps.<br>"
        f"- The size of the marker is based on the percentage of the race a driver was within {gap_margin} second(s) of another driver (on same lap - can be > 100% if battling > 1 driver within a sector).</sup>",
        xaxis_title="Average (s)",
        yaxis_title="Variance (s)",
        plot_bgcolor="rgba(0,0,0,0)",  # Set the plot background color to transparent
        paper_bgcolor="#2c2c2c",  # Set the paper background color to match the site's background color
        font=dict(
            size=14, color="#e0e0e0"
        ),  # Set the font color to match the site's text color
        hovermode="closest",
        xaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
        yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)", range=[0, None]),
    )

    tickvals = np.linspace(df["PercentDiff"].min(), df["PercentDiff"].max(), num=10)
    fig.update_layout(
        xaxis=dict(
            tickmode="array",
            tickvals=tickvals,
            ticktext=[
                f"{val:.0%} - {df.loc[(df['PercentDiff'] - val).abs().idxmin(), 'AverageLapTime']:.2f}"
                for val in tickvals
            ],
        )
    )

    # fig.update_layout(
    #     title=f"Driver Proximity Analysis<br>"
    #     f"<sup>% of race within {gap_margin} second(s) of other drivers on the same lap (can be > 100% if in proximity of > 1 driver during a sector)</sup>",
    #     xaxis_title="Average Lap Time (s)",
    #     yaxis_title="Variance (s)",
    #     # xaxis_title="Finish Position",
    #     # yaxis_title="Gap to Leader (s)",
    #     plot_bgcolor="rgba(0,0,0,0)",  # Set the plot background color to transparent
    #     paper_bgcolor="#2c2c2c",  # Set the paper background color to match the site's background color
    #     font=dict(
    #         size=14, color="#e0e0e0"
    #     ),  # Increase font size and set the font color to match the site's text color
    #     hovermode="closest",
    #     xaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
    #     yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
    # )

    return fig


def plot_laps_in_session(session: Session) -> go.Figure:
    """plot all drivers' laps per lap"""
    fig = go.Figure()
    fig.update_layout(
        title=f"`{session.track_name}` `{session.session_file}`",
        xaxis_title="Lap",
        yaxis_title="Time (s)",
        plot_bgcolor="rgba(0,0,0,0)",  # Set the plot background color to transparent
        paper_bgcolor="#2c2c2c",  # Set the paper background color to match the site's background color
        font=dict(color="#e0e0e0"),  # Set the font color to match the site's text color
        hovermode="closest",
        xaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
        yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
    )

    session.drivers.sort(
        key=lambda d: (
            d.car.avg_percent_diff if d.car and d.car.avg_percent_diff else float("inf")
        )
    )

    starting_colors = [
        (255, 204, 0),  # Highlight color
        (255, 77, 77),  # Button color
        # (255, 51, 51),  # Hover color
        (224, 224, 224),  # Text color
        # (44, 44, 44),  # Background color
        (0, 122, 204),  # blue
        (0, 204, 102),  # green
    ]
    colors = []
    for d_idx, driver in enumerate(session.drivers):
        # starting color
        base_color = starting_colors[d_idx % len(starting_colors)]

        # fade opacity based on how far down the list the driver is
        min_opacity = 0.3
        opacity = (
            1
            - (
                (d_idx // len(starting_colors))
                / (len(session.drivers) / len(starting_colors) - 1)
            )
        ) * min_opacity + min_opacity
        color = f"rgba({base_color[0]}, {base_color[1]}, {base_color[2]}, {opacity})"
        colors.append(color)

    for d_idx, driver in enumerate(session.drivers):
        if not driver.laps:
            continue

        laps: list[Lap] = []
        for l_idx, lap in enumerate(driver.laps):
            if not (
                lap.lap_number in driver.probable_pit_laps
                or lap.lap_number + 1 in driver.probable_pit_laps
                or lap.lap_number == 1
            ):
                laps.append(lap)
            elif lap.lap_number in driver.probable_pit_laps:
                fig.add_trace(
                    go.Scatter(
                        x=[lap.lap_number],
                        y=[driver.best_lap.lap_time / 1000.0 + 3],
                        mode="markers",
                        name=f"{driver.name} - Pit L{lap.lap_number}",
                        text=f"{driver.name} - Pit L{lap.lap_number}",
                        marker=dict(color=colors[d_idx], size=10),
                    )
                )

        for s_idx, stint in enumerate(driver.stints):
            if not stint.laps:
                break
            slope = stint.trend
            intercept = stint.laps[0].lap_time - slope * stint.laps[0].lap_number
            trendline = [
                (slope / 1000.0 * l.lap_number + intercept / 1000.0) for l in stint.laps
            ]
            fig.add_trace(
                go.Scatter(
                    x=[lap.lap_number for lap in stint.laps],
                    y=trendline,
                    mode="lines",
                    name=f"{driver.name} Stint {s_idx + 1} Trend",
                    line=dict(color=colors[d_idx], dash="dash"),
                )
            )

        fig.add_trace(
            go.Scatter(
                x=[lap.lap_number for lap in laps],
                y=[lap.lap_time / 1000.0 for lap in laps],
                mode="lines+markers",
                name=f"{driver.name} Laps",
                text=[
                    f"L{lap.lap_number} {driver.name} - {lap.lap_time / 1000.0:.3f}"
                    for lap in laps
                ],
                marker=dict(color=colors[d_idx]),
            )
        )

    return fig


def plot_valid_invalid_analysis(
    sessions: list[Session],
    divs_to_show: set[int] = set(),
    do_sort_by_division: bool = True,
) -> go.Figure:
    """stacked bar chart of count of valid and invalid laps per driver"""
    fig = go.Figure()

    fig.update_layout(
        title=f"Valid vs Invalid Laps per Driver<br>",
        # f"<sup>Percentages and counts based on the middle 50% of a driver's laps.</sup>",
        xaxis_title="Lap Count",
        yaxis_title="Driver",
        plot_bgcolor="rgba(0,0,0,0)",  # Set the plot background color to transparent
        paper_bgcolor="#2c2c2c",  # Set the paper background color to match the site's background color
        font=dict(color="#e0e0e0"),  # Set the font color to match the site's text color
        hovermode="closest",
        xaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
        yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
        barmode="stack",
    )

    career_drivers: list[CareerDriver] = list(
        filter(
            lambda cd: not divs_to_show or cd.driver.race_division in divs_to_show,
            CareerDriver.from_sessions(sessions),
        )
    )

    for career_driver in career_drivers:
        career_driver.laps_25_to_50 = sorted(
            career_driver.laps, key=lambda l: l.lap_time
        )[len(career_driver.laps) // 4 : len(career_driver.laps) * 2 // 4]
        if career_driver.laps_25_to_50:
            career_driver.mean_25_to_50 = statistics.mean(
                l.lap_time for l in career_driver.laps_25_to_50
            )
        else:
            career_driver.mean_25_to_50 = float("inf")

    career_drivers.sort(
        key=lambda cd: (
            (cd.driver.race_division if do_sort_by_division else float("inf")),
            cd.mean_25_to_50,
            # len(cd.laps),
        )
    )

    for cd_idx, career_driver in enumerate(career_drivers):
        if not career_driver.laps_25_to_50:
            continue
        driver = career_driver.driver

        valid_laps = [l for l in career_driver.laps_25_to_50 if l.is_valid_for_best]
        invalid_laps = [
            l for l in career_driver.laps_25_to_50 if not l.is_valid_for_best
        ]

        valid_count = len(valid_laps)
        invalid_count = len(invalid_laps)
        total_count = valid_count + invalid_count

        valid_percentage = (valid_count / total_count) * 100 if total_count else 0
        invalid_percentage = (invalid_count / total_count) * 100 if total_count else 0

        division_color = SRA_DIV_COLOR.from_division(driver.race_division)
        driver_color = (
            division_color.apply_silver_tint()
            if driver.is_silver_driver
            else division_color
        )
        name = f"{driver.name} ({driver.car.model.name}) - {career_driver.mean_25_to_50 / 1000.0:.3f}"

        if valid_count:
            fig.add_trace(
                go.Bar(
                    x=[valid_count],
                    y=[cd_idx],
                    name=f"V: {valid_count} - {name}",
                    text=f"{driver.name} Valid: {valid_count} ({valid_percentage:.2f}%) - {statistics.mean([l.lap_time for l in valid_laps]) / 1000.0:.3f}",
                    marker_color=driver_color.to_rgba(),
                    marker_line=dict(color=driver_color.to_rgba()),
                    orientation="h",
                )
            )

        if invalid_count:
            fig.add_trace(
                go.Bar(
                    x=[invalid_count],
                    y=[cd_idx],
                    name=f"I: {invalid_count} - {name}",
                    text=f"{driver.name} Invalid: {invalid_count} ({invalid_percentage:.2f}%) - {statistics.mean([l.lap_time for l in invalid_laps]) / 1000.0:.3f}",
                    marker_color=driver_color.darken().to_rgba(),
                    marker_line=dict(color=driver_color.darken().to_rgba()),
                    orientation="h",
                )
            )

    fig.update_yaxes(autorange="reversed")

    return fig


def plot_best_laps(
    sessions: list[Session],
    lap_attr: str = "lap_time",
    divs_to_show: set[int] = set(),
    do_sort_by_division: bool = True,
) -> go.Figure:
    """lap_attr is used to determine if we want lap_time, split1, split2, or split3"""
    fig = go.Figure()
    fig.update_layout(
        title=f"{sessions[0].track_name.title()}'s Best {lap_attr.replace('_', ' ').title()}s after {sessions[0].finish_time.strftime('%Y-%m-%d')}",
        xaxis_title="Time (s)",
        yaxis_title="Rank",
        plot_bgcolor="rgba(0,0,0,0)",  # Set the plot background color to transparent
        paper_bgcolor="#2c2c2c",  # Set the paper background color to match the site's background color
        font=dict(color="#e0e0e0"),  # Set the font color to match the site's text color
        hovermode="closest",
        xaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
        yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
        barmode="overlay",
    )
    all_laps: list[Lap] = []
    driver_car_laps_dict: dict[str, tuple[list[SessionDriver], list[Lap]]] = {}
    # driver_id -> (Driver, [(Lap, time)])
    for session in sessions:
        for driver in session.drivers:
            # not show divs means show all divs, otherwise check if race division is in show_divs
            if not (not divs_to_show or driver.race_division in divs_to_show):
                continue

            if driver.car.car_group != "GT3":
                continue

            driver_car_id: str = f"{driver.driver_id}_{driver.car.car_model}"
            if not driver_car_id in driver_car_laps_dict:
                driver_car_laps_dict[driver_car_id] = ([], [])
            driver_car_laps_dict[driver_car_id][0].append(driver)

            for lap in driver.laps:
                driver_car_laps_dict[driver_car_id][1].append(lap)
                all_laps.append(lap)

    career_drivers: dict[str, tuple[CareerDriver, list[Lap]]] = (
        {}
    )  # driver_car_id -> (CareerDriver, [Lap])
    for driver_car_id, driver_car_laps_dict in driver_car_laps_dict.items():
        drivers: list[SessionDriver] = driver_car_laps_dict[0]
        laps: list[Lap] = driver_car_laps_dict[1]
        career_driver: CareerDriver = CareerDriver.from_drivers(drivers)
        if career_driver.best_laps:
            career_drivers[driver_car_id] = (
                career_driver,
                sorted(
                    laps,
                    key=lambda l: (
                        0 if l.is_valid_for_best else 1,
                        getattr(l, lap_attr),
                    ),
                ),
            )

    lap_attr_is_lap_time: bool = getattr(all_laps[0], lap_attr) == all_laps[0].lap_time

    all_laps = sorted(all_laps, key=lambda l: getattr(l, lap_attr))
    times = [getattr(lap, lap_attr) for lap in all_laps if lap.is_valid_for_best]
    mean_time = statistics.mean(times)
    median_time = statistics.median(times)
    variance = statistics.variance(times)
    stdev = statistics.stdev(times)
    std_dev = 1
    min_value = min(times[0] * 0.995, median_time - std_dev * stdev)
    max_value = median_time + std_dev * stdev
    min_value = times[0] * 0.995
    max_value = times[0] * 1.05

    fig.update_yaxes(autorange="reversed")
    fig.update_xaxes(range=[min_value / 1000.0, max_value / 1000.0])

    # Add vertical lines for statistics values
    # fig.add_vline(
    #     x=mean_time / 1000.0,
    #     line=dict(color="blue", dash="dash"),
    #     annotation_text=f"Mean {mean_time / 1000.0:.3f}",
    #     annotation_position="bottom right",
    # )
    # fig.add_vline(
    #     x=median_time / 1000.0,
    #     line=dict(color="green", dash="dash"),
    #     annotation_text=f"Median (of all valid practice times for {', '.join(f'D{d}' for d in divs_to_show) if divs_to_show else 'all divs'}) {median_time / 1000.0:.3f}",
    #     annotation_position="bottom left",
    # )
    # fig.add_vline(
    #     x=(median_time - stdev) / 1000.0,
    #     line=dict(color="red", dash="dash"),
    #     annotation_text=f"-1 Std Dev {(median_time - stdev) / 1000.0:.3f}",
    #     annotation_position="bottom right",
    # )
    # fig.add_vline(
    #     x=(median_time + stdev) / 1000.0,
    #     line=dict(color="red", dash="dash"),
    #     annotation_text=f"+1 Std Dev {(median_time + stdev) / 1000.0:.3f}",
    #     annotation_position="bottom left",
    # )

    # filter out the laps that are outside of min/max range
    # all_laps = filter(
    #     lambda l: min_value <= getattr(l, lap_attr) <= max_value, all_laps
    # )

    career_drivers = dict(
        sorted(
            career_drivers.items(),
            key=lambda d: (
                (
                    d[1][0].driver.race_division
                    if do_sort_by_division and d[1][0].driver.division
                    else float("inf")
                ),
                getattr(d[1][1][0], lap_attr),
                # d[1] is value, [1] is the list of laps, [0] is the first lap
            ),
        )
    )

    # Add vertical lines for every 1% over the min time
    percent_step = 0.01
    current_percent = 1.0
    fasest_valid_time: int = getattr(
        [l for l in list(career_drivers.values())[0][1]][0], lap_attr
    )
    while fasest_valid_time * current_percent <= max_value:
        fig.add_vline(
            x=(fasest_valid_time * current_percent) / 1000.0,
            line=dict(color="blue", dash="dash"),
            annotation_text=f"{int(current_percent * 100)}%",
            annotation_position="top right",
        )
        current_percent += percent_step

    for cd_idx, driver_car_id in enumerate(career_drivers):
        career_driver = career_drivers[driver_car_id][0]
        laps = career_drivers[driver_car_id][1]
        if laps[0] is None:
            continue
        valid_laps: list[Lap] = []
        invalid_laps: list[Lap] = []
        for lap in laps:
            if lap.is_valid_for_best:
                valid_laps.append(lap)
            else:
                invalid_laps.append(lap)
        best_time = min(
            getattr(invalid_laps[0], lap_attr) if invalid_laps else float("inf"),
            getattr(valid_laps[0], lap_attr) if valid_laps else float("inf"),
        )
        division_color = SRA_DIV_COLOR.from_division(career_driver.driver.race_division)
        driver_color = (
            division_color.apply_silver_tint()
            if career_driver.driver.is_silver_driver
            else division_color
        )
        best_time_color: SRA_DIV_COLOR = (
            driver_color if not driver_color.is_bright() else driver_color.darken()
        ).darken()
        if True and valid_laps:
            best_valid_time = getattr(valid_laps[0], lap_attr)
            fig.add_trace(
                go.Bar(
                    x=[best_valid_time / 1000.0],
                    y=[cd_idx + 1],
                    name=f"{career_driver.driver.division} | {career_driver.driver.name} (VB)",
                    text=f"Valid Best: {career_driver.driver.division} | {career_driver.driver.name} {best_valid_time / 1000.0:.3f}",
                    marker_color=driver_color.to_rgba(),
                    marker_line=dict(color=driver_color.to_rgba()),
                    orientation="h",
                    width=0.8,
                )
            )

            car_file_path = career_driver.driver.car.model.get_logo_path(
                current_dir=os.path.dirname(__file__)
            )
            # FIXME these should be stored in ram
            # img = Image.open(car_file_path)
            # aspect_ratio = img.height / img.width
            # img_x = max(best_time, best_valid_time) / 1000.0
            # fig.add_layout_image(
            #     dict(
            #         source=img,
            #         xref="x",
            #         yref="y",
            #         x=img_x,
            #         y=cd_idx + 1,
            #         sizex=1,
            #         sizey=1 * aspect_ratio,
            #         xanchor="left",
            #         yanchor="middle",
            #     )
            # )

        fig.add_trace(
            go.Bar(
                x=[best_time / 1000.0],
                y=[cd_idx + 1],
                name=f"{career_driver.driver.division} | {career_driver.driver.name} (B)",
                text=f"Best: {career_driver.driver.division} | {career_driver.driver.name} {best_time / 1000.0:.3f}",
                marker_color=best_time_color.to_rgba(),
                marker_line=dict(color=best_time_color.to_rgba()),
                orientation="h",
                width=0.8,
            )
        )

        # check to see if lap_attr is getting the lap time of the lap (not any of the splits)
        if lap_attr_is_lap_time:
            if career_driver.potential_best_valid_lap_time:
                fig.add_trace(
                    go.Bar(
                        x=[career_driver.potential_best_valid_lap_time / 1000.0],
                        y=[cd_idx + 1],
                        name=f"{career_driver.driver.division} | {career_driver.driver.name} (VPB)",
                        text=f"Valid Potential Best: {career_driver.driver.division} | {career_driver.driver.name} {career_driver.potential_best_valid_lap_time / 1000.0:.3f}",
                        marker_color=driver_color.darken().to_rgba(),
                        marker_line=dict(color=driver_color.darken().to_rgba()),
                        orientation="h",
                        width=0.8,
                    )
                )

            if career_driver.potential_best_lap_time:
                fig.add_trace(
                    go.Bar(
                        x=[career_driver.potential_best_lap_time / 1000.0],
                        y=[cd_idx + 1],
                        name=f"{career_driver.driver.division} | {career_driver.driver.name} (PB)",
                        text=f"Potential Best: {career_driver.driver.division} | {career_driver.driver.name} {career_driver.potential_best_lap_time / 1000.0:.3f}",
                        marker_color=best_time_color.darken().to_rgba(),
                        marker_line=dict(color=best_time_color.darken().to_rgba()),
                        orientation="h",
                        width=0.8,
                    )
                )

    return fig


def plot_avg_best_lap(
    sessions: list[Session],
    top_n_laps: int = 3,
    divs_to_show: set[int] = set(),
    do_sort_by_division: bool = True,
) -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        title=f"{sessions[0].track_name.title()}'s Average Top {top_n_laps} Session Bests after {sessions[0].finish_time.strftime('%Y-%m-%d')}",
        xaxis_title="Time (s)",
        yaxis_title="Rank",
        plot_bgcolor="rgba(0,0,0,0)",  # Set the plot background color to transparent
        paper_bgcolor="#2c2c2c",  # Set the paper background color to match the site's background color
        font=dict(color="#e0e0e0"),  # Set the font color to match the site's text color
        hovermode="closest",
        xaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
        yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
        barmode="overlay",
    )

    driver_car_dict: dict[str, list[SessionDriver]] = (
        {}
    )  # driver_id_car_model_id -> [SessionDriver]
    for s_idx, session in enumerate(sessions):
        for driver in session.drivers:
            driver_car_id = f"{driver.driver_id}_{driver.car.car_model}"
            if driver_car_id not in driver_car_dict:
                driver_car_dict[driver_car_id] = []
            driver_car_dict[driver_car_id].append(driver)

    career_drivers: list[CareerDriver] = []
    # (career_driver, lap_time, is_valid, laps counted)
    for ds in driver_car_dict.values():
        career_driver: CareerDriver = CareerDriver.from_drivers(ds)
        # not show divs means show all divs, otherwise check if race division is in show_divs
        if not (not divs_to_show or career_driver.driver.race_division in divs_to_show):
            continue
        if not career_driver.best_laps:
            continue
        career_driver.avg_best_lap_time(count=top_n_laps)
        career_driver.avg_best_valid_lap_time(count=top_n_laps)
        career_drivers.append(career_driver)

    career_drivers.sort(
        key=lambda cd: (
            (
                cd.driver.race_division
                if do_sort_by_division and cd.driver.division
                else float("inf")
            ),
            (
                cd.avg_best_valid_lap_time()
                if cd.avg_best_valid_lap_time()
                else cd.avg_best_lap_time() if cd.avg_best_lap_time() else float("inf")
            ),
        )
    )

    fig.update_yaxes(autorange="reversed")
    fig.update_xaxes(
        range=[
            career_drivers[0].avg_best_lap_time() / 1000.0 * 0.995,
            career_drivers[0].avg_best_lap_time() / 1000.0 * 1.05,
        ]
    )

    images: dict[str, ImageFile] = {}
    for cd_idx, career_driver in enumerate(career_drivers):
        division_color = SRA_DIV_COLOR.from_division(career_driver.driver.race_division)
        driver_color = (
            division_color.apply_silver_tint()
            if career_driver.driver.is_silver_driver
            else division_color
        )
        if career_driver.avg_best_valid_lap_time():
            num_laps = min(top_n_laps, len(career_driver.best_laps))
            fig.add_trace(
                go.Bar(
                    x=[career_driver.avg_best_valid_lap_time() / 1000.0],
                    y=[cd_idx + 1],
                    name=f"{career_driver.driver.division} | {career_driver.driver.name} (BV - {num_laps})",
                    text=f"Best Valid: {career_driver.driver.division} | {career_driver.driver.name} {career_driver.avg_best_valid_lap_time() / 1000.0:.3f}<br>Top {num_laps} Session Bests: {', '.join([f'{lap.lap_time / 1000.0:.3f}' for lap in career_driver.best_valid_laps][:num_laps])}",
                    marker_color=driver_color.to_rgba(),
                    marker_line=dict(color=driver_color.to_rgba()),
                    orientation="h",
                    width=0.8,
                )
            )
        lap_time_color = (
            driver_color if not driver_color.is_bright() else driver_color.darken()
        ).darken()
        num_laps = min(top_n_laps, len(career_driver.best_laps))
        fig.add_trace(
            go.Bar(
                x=[career_driver.avg_best_lap_time() / 1000.0],
                y=[cd_idx + 1],
                name=f"{career_driver.driver.division} | {career_driver.driver.name} (B - {num_laps})",
                text=f"Best: {career_driver.driver.division} | {career_driver.driver.name} {career_driver.avg_best_lap_time() / 1000.0:.3f}<br>Top {num_laps} Session Bests: {', '.join([f'{lap.lap_time / 1000.0:.3f}' for lap in career_driver.best_laps][:num_laps])}",
                marker_color=lap_time_color.darken().to_rgba(),
                marker_line=dict(color=lap_time_color.darken().to_rgba()),
                orientation="h",
                width=0.8,
            )
        )

        car_file_path = career_driver.driver.car.model.get_logo_path(
            current_dir=os.path.dirname(__file__)
        )
        try:
            if car_file_path not in images:
                images[car_file_path] = Image.open(car_file_path)
            img = images[car_file_path]
            aspect_ratio = img.height / img.width
            fig.add_layout_image(
                dict(
                    source=img,
                    xref="x",
                    yref="y",
                    x=(
                        max(
                            career_driver.avg_best_lap_time(),
                            career_driver.avg_best_valid_lap_time(),
                        )
                        if career_driver.avg_best_valid_lap_time()
                        else career_driver.avg_best_lap_time()
                    )
                    / 1000.0,
                    y=cd_idx + 1,
                    sizex=1,
                    sizey=1 * aspect_ratio,
                    xanchor="left",
                    yanchor="middle",
                )
            )
        except FileNotFoundError:
            raise FileNotFoundError(f"File not found: {car_file_path}")

    return fig


def plot_activity() -> go.Figure:
    fig = go.Figure()
    return fig


def plot_time_in_pits(race_sessions: list[Session], sort_by_div=True) -> go.Figure:
    fig = go.Figure()
    race_drivers: list[SessionDriver] = sorted(
        [d for s in race_sessions for d in s.drivers],
        key=lambda d: (
            d.race_division if sort_by_div else float("inf"),
            d.time_in_pits if d.time_in_pits else float("inf"),
        ),
    )

    for d_idx, driver in enumerate(race_drivers):
        if not driver.time_in_pits:
            continue

        if driver.start_offset < -10:
            continue

        div_color = SRA_DIV_COLOR.from_division(driver.race_division)
        driver_color = (
            div_color.apply_silver_tint() if driver.is_silver_driver else div_color
        ).darken()
        fig.add_trace(
            go.Bar(
                x=[f"{driver.name} - {driver.time_in_pits / 1000.0:.3f}"],
                y=[driver.time_in_pits / 1000.0],
                name=f"{driver.name}",
                marker_color=driver_color.to_rgba(),
                text=[f"{driver.name} - {driver.time_in_pits / 1000.0}"],
                textposition="outside",
                orientation="v",
                # width=0.8,
            )
        )

    # fig.update_yaxes(autorange="reversed")

    fig.update_layout(
        title=f"{race_sessions[0].track_name} Pit Times",
        xaxis_title="Rank",
        yaxis_title="Time (s)",
        plot_bgcolor="rgba(0,0,0,0)",  # Set the plot background color to transparent
        paper_bgcolor="#2c2c2c",  # Set the paper background color to match the site's background color
        font=dict(color="#e0e0e0"),  # Set the font color to match the site's text color
        hovermode="closest",
        xaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
        yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
    )

    return fig


def plot_quali_analysis(sessions: list[Session]) -> go.Figure:
    """
    two plots, one plot to show the lap number that best laps were set on, bar plot
    another plot to show session lap count when best laps are set
    """
    fig = go.Figure()
    fig.update_layout(
        title=f"Qualifying Analysis",
        xaxis_title="Lap Number",
        yaxis_title="Count",
        plot_bgcolor="rgba(0,0,0,0)",  # Set the plot background color to transparent
        paper_bgcolor="#2c2c2c",  # Set the paper background color to match the site's background color
        font=dict(color="#e0e0e0"),  # Set the font color to match the site's text color
        hovermode="closest",
        xaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
        yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
    )

    driver_lap_count: dict[int, int] = {}  # lap number -> count
    session_lap_count: dict[int, int] = {}  # session lap count -> count
    for session in sessions:
        for d_idx, driver in enumerate(session.drivers):
            for l_idx, lap in enumerate([driver.best_lap, driver.best_valid_lap][1:]):
                if not lap:
                    continue
                if lap.lap_number not in driver_lap_count:
                    driver_lap_count[lap.lap_number] = 0
                if lap.running_session_lap_count not in session_lap_count:
                    session_lap_count[lap.running_session_lap_count] = 0
                driver_lap_count[lap.lap_number] += 1
                session_lap_count[lap.running_session_lap_count] += 1

    fig.add_trace(
        go.Bar(
            x=list(driver_lap_count.keys()),
            y=list(driver_lap_count.values()),
            name="Lap Number",
            marker_color="rgba(255, 204, 0, 0.5)",  # Highlight color
        )
    )
    max_lap_number = max(driver_lap_count.keys())
    max_session_lap_count = max(session_lap_count.keys())
    bucket_size = max_session_lap_count / max_lap_number

    fig.add_trace(
        go.Histogram(
            x=list(session_lap_count.keys()),
            y=list(session_lap_count.values()),
            name="Session Lap Count",
            xbins=dict(size=bucket_size),
            marker_color="rgba(0, 122, 204, 0.5)",  # Blue color
        )
    )

    return fig


def plot_laps_vs_num_laps_in_session():
    fig = go.Figure()

    # http://localhost:5000/api/laps?afterDate=2025-01-25&beforeDate=2025-01-30&trackName=mount_panorama
    url = "http://localhost:5000/api/laps?afterDate=2025-01-25&beforeDate=2025-01-30&trackName=mount_panorama"
    response = requests.get(url)
    data = response.json()

    # {
    #     "key_": "250125_001654_FP_3_250125_001654_FP_3_1005_1",
    #     "lapNumber": 1,
    #     "lapTime": 122120,
    #     "split1": 38527,
    #     "split2": 56715,
    #     "split3": 26877,
    #     "isValidForBest": true,
    #     "running_session_lap_count": 0,
    #     "serverNumber": "3",
    #     "sessionFile": "250125_001654_FP",
    #     "car": {
    #       "key_": "250125_001654_FP_3_1005",
    #       "carId": 1005,
    #       "carModel": {
    #         "modelId": 36,
    #         "name": "Ford Mustang GT3",
    #         "logoPath": "../assets/images/logo/manufacturers/Ford_Mustang_GT3.png"
    #       },
    #       "carNumber": 365,
    #       "finishPosition": 1,
    #       "totalTime": 95022055,
    #       "timeInPits": 0,
    #       "lapCount": 2,
    #       "bestSplit1": 38307,
    #       "bestSplit2": 56340,
    #       "bestSplit3": 26790,
    #       "bestLap": 121437,
    #       "avgPercentDiff": null,
    #       "tsAvgPercentDiff": null
    #     },
    #     "driver": {
    #       "driverId": "S76561199044769565",
    #       "memberId": "aa4cf32684443d1835944d7b2f40ba15c68b2a7b03441f469ca3661db2e3d293",
    #       "firstName": "Justin",
    #       "lastName": "Moffatt",
    #       "division": 4,
    #       "raceDivision": 4,
    #       "isSilver": false,
    #       "name": "Justin Moffatt",
    #       "sraMemberStatsURL": "https://www.simracingalliance.com/member_stats/?member=aa4cf32684443d1835944d7b2f40ba15c68b2a7b03441f469ca3661db2e3d293"
    #     },
    #     "carDriver": {
    #       "sessionFile": "250125_001654_FP",
    #       "serverNumber": "3",
    #       "carID": 1005,
    #       "driverId": "S76561199044769565",
    #       "carKey": "250125_001654_FP_3_1005",
    #       "key_": "250125_001654_FP_3_1005_S76561199044769565",
    #       "timeOnTrack": 0,
    #       "carDriverKey": "Ford Mustang GT3-S76561199044769565"
    #     },
    #     "session": {
    #       "key": "250125_001654_FP_3",
    #       "trackName": "mount_panorama",
    #       "sessionType": "FP",
    #       "finishTime": "2025-01-25T00:16:54.000Z",
    #       "sessionFile": "250125_001654_FP",
    #       "serverNumber": "3",
    #       "serverName": "#SRAggTT | GT3_FreePractice | Q | SimRacingAlliance.com | #SRAM3 | cBOP",
    #       "sraSessionURL": "https://www.simracingalliance.com/results/server3/practice/250125_001654_FP",
    #       "sessionTypeSraWord": "practice"
    #     }
    #   },
    lap_times_by_division = {}
    session_lap_count_by_division = {}

    for lap in data:
        division = lap["driver"]["raceDivision"]
        if division not in lap_times_by_division:
            lap_times_by_division[division] = []
            session_lap_count_by_division[division] = []

        if not lap["isValidForBest"]:
            continue

        if lap["lapTime"] > 125000:
            continue

        lap_times_by_division[division].append(lap["lapTime"] / 1000.0)
        session_lap_count_by_division[division].append(lap["running_session_lap_count"])

    for division in sorted(lap_times_by_division.keys()):
        fig.add_trace(
            go.Scatter(
                x=session_lap_count_by_division[division],
                y=lap_times_by_division[division],
                mode="markers",
                marker=dict(size=10, opacity=0.7),
                name=f"Division {division}",
                text=[
                    f"Lap {i+1}: {time:.3f}s"
                    for i, time in enumerate(lap_times_by_division[division])
                ],
                hoverinfo="text",
            )
        )

        # Add trendline
        slope, intercept, _, _, _ = linregress(
            session_lap_count_by_division[division], lap_times_by_division[division]
        )
        trendline = [
            slope * x + intercept for x in session_lap_count_by_division[division]
        ]
        fig.add_trace(
            go.Scatter(
                x=session_lap_count_by_division[division],
                y=trendline,
                mode="lines",
                line=dict(dash="dash"),
                name=f"Division {division} Trendline (y = {slope:.3f}x + {intercept:.3f})",
            )
        )

    fig.update_layout(
        title="Lap Times vs Number of Laps in Session by Division",
        xaxis_title="Number of Laps in Session",
        yaxis_title="Lap Time (s)",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="#2c2c2c",
        font=dict(color="#e0e0e0"),
        hovermode="closest",
        xaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
        yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
    )

    fig.update_yaxes(range=[118, 126])

    fig.show()

    pass


def plot_car_finish_positions():
    # season,track,division,car_model,finish_position
    car_finish_positions = pd.read_csv("car_finish_positions.csv")
    car_models = CarModels.model_id_dict
    car_finish_positions["car_model"] = car_finish_positions["car_model"].map(
        car_models
    )

    # a race is a (season, track, division)
    # {car_model: [ race ]}
    car_model_races: dict[str, list[dict]] = {}
    for idx, row in car_finish_positions.iterrows():
        race = {
            "season": row["season"],
            "track": row["track"],
            "division": row["division"],
        }
        car_model = row["car_model"]
        if car_model not in car_model_races:
            car_model_races[car_model] = []
        car_model_races[car_model].append(race)

    car_finish_positions_grouped = (
        car_finish_positions.groupby(["car_model", "finish_position"])
        .sum()
        .reset_index()
    )

    car_finish_positions_grouped.to_csv("car_finish_positions_grouped.csv")

    for car_model, races in car_model_races.items():
        num_races = len(races)
        car_finish_positions_grouped.loc[
            car_finish_positions_grouped["car_model"] == car_model, "percent_finished"
        ] = (
            car_finish_positions_grouped.loc[
                car_finish_positions_grouped["car_model"] == car_model, "season"
            ]
            / num_races
            * 100
        )
    fig = go.Figure()

    car_finish_positions_grouped = car_finish_positions_grouped.pivot(
        index="car_model", columns="finish_position", values="percent_finished"
    ).fillna(0)

    for car_model in car_finish_positions_grouped.index:
        fig.add_trace(
            go.Bar(
                x=car_finish_positions_grouped.columns,
                y=car_finish_positions_grouped.loc[car_model],
                name=car_model,
                text=[
                    f"{car_model} - {val:.2f}%"
                    for val in car_finish_positions_grouped.loc[car_model]
                ],
                textposition="auto",
            )
        )

    fig.update_layout(
        title="Finish Position Rate by Car Model",
        xaxis_title="Finish Position",
        yaxis_title="Rate (%)",
        barmode="stack",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="#2c2c2c",
        font=dict(color="#e0e0e0"),
        hovermode="closest",
        xaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
        yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
    )

    fig.show()

    pass


if __name__ == "__main__":
    plot_car_finish_positions()
    # plot_laps_vs_num_laps_in_session()
    # practice_sessions_pickle = "practice_sessions.pkl"
    # if True or not os.path.exists(practice_sessions_pickle):
    #     neo_driver, neo_session = Neo4jDatabase.connect_database("SRA")
    #     Neo4jDatabase.close_connection(neo_driver, neo_session)
    #     with open(practice_sessions_pickle, "wb") as f:
    #         pickle.dump(sessions, f)

    # else:
    #     with open(practice_sessions_pickle, "rb") as f:
    #         sessions = pickle.load(f)

    # plot_laps(sessions)

    queries_pickle = "queries.pkl"
    tracks = [
        # TrackNames.SUZUKA.value,
        # TrackNames.WATKINS_GLEN.value,
        # TrackNames.MISANO.value,
        # TrackNames.NURBURGRING.value,
        TrackNames.ZOLDER.value,
    ]

    if False or not os.path.exists(queries_pickle):
        neo_driver, neo_session = Neo4jDatabase.connect_database("SRA")

        # sessions = get_complete_sessions_for_track(
        #     neo_driver=neo_driver,
        #     # track_name=TrackNames.KYALAMI.value,
        #     # track_name=TrackNames.MISANO.value,
        #     track_name=TrackNames.WATKINS_GLEN.value,
        #     after_date=datetime.now() - timedelta(days=10),
        # )

        # ts_weekend = get_team_series_weekend_by_attrs(
        #     neo_driver=neo_driver,
        #     season=13,
        #     track_name=TrackNames.MISANO.value,
        #     # track_name=TrackNames.SUZUKA.value,
        #     # track_name=TrackNames.WATKINS_GLEN.value,
        #     # track_name=TrackNames.ZANDVOORT.value,
        #     division=2,
        # )
        # ts_weekend.set_drivers(neo_driver)

        race_sessions_by_track: dict[str, list[Session]] = {}  # track_name -> [Session]
        for track in tracks:
            race_sessions_by_track[track] = []
            for div in list(range(1, 7))[:]:
                ts_weekend = get_team_series_weekend_by_attrs(
                    neo_driver=neo_driver,
                    season=13,
                    track_name=track,
                    division=div,
                )
                if not ts_weekend:
                    continue
                ts_weekend.set_drivers(neo_driver)
                race_sessions_by_track[track].append(ts_weekend.race)

        # race_drivers = get_sra_drivers(neo_driver, session_types={"R"})

        # sessions = get_complete_sessions_for_track(
        #     neo_driver=neo_driver,
        #     track_name=TrackNames.NURBURGRING.value,
        #     after_date=datetime(2024, 12, 17),
        # )

        # sessions = get_complete_sessions_by_keys(
        #     neo_driver=neo_driver,
        #     session_keys={
        #         ts.session.key_
        #         for ts in get_team_series_sessions_by_attrs(
        #             neo_driver=neo_driver,
        #             seasons={12},
        #             # divisions={1, 2, 3},
        #             session_types={"Q"},
        #         )
        #     },
        # )

        # session = get_session_by_key(
        #     neo_driver=neo_driver, session_key="250111_232140_R_7"
        # )
        # drivers = (
        #     session.set_session_drivers(neo_driver).set_driver_gaps_to_leader().drivers
        # )

        Neo4jDatabase.close_connection(neo_driver, neo_session)
        with open(queries_pickle, "wb") as f:
            pickle.dump(race_sessions_by_track, f)

    else:
        with open(queries_pickle, "rb") as f:
            race_sessions_by_track = pickle.load(f)

    # fig = plot_total_times(ts_weekend.race.session.drivers)
    # fig = plot_race(ts_weekend)
    # fig = plot_apds(race_drivers, divs_to_show={}, last_n_sessions=5)
    # fig = plot_laps_in_session(race_sessions_by_track[tracks[0]][0])
    # fig = plot_quali_analysis(sessions)
    # fig = plot_avg_best_lap(
    #     sessions,
    #     divs_to_show={
    #         *range(1, 7),
    #     },
    #     do_sort_by_division=False,
    #     top_n_laps=3,
    # )
    fig = plot_time_in_pits(race_sessions_by_track[tracks[0]], sort_by_div=True)
    # dfs_by_track: dict[str, pd.DataFrame] = {}
    # drivers: dict[SessionDriver, list[float]] = {}  # SessionDriver -> [PercentDiff]
    # for track in tracks:
    #     # fig = plot_pace_evaluation(
    #     #     [d for s in race_sessions_by_track[track] for d in s.drivers],
    #     #     include_lapped_drivers=True,
    #     # )
    #     df = plot_pace_evaluation(
    #         [d for s in race_sessions_by_track[track] for d in s.drivers],
    #         include_lapped_drivers=True,
    #     )
    #     dfs_by_track[track] = df
    #     for row in df.itertuples():
    #         if row.Driver not in drivers:
    #             drivers[row.Driver] = []
    #         drivers[row.Driver].append(row.PercentDiff)

    # combined_df = pd.concat(dfs_by_track.values())
    # combined_df = combined_df.groupby("Driver").mean().reset_index()

    # for driver in drivers:
    #     combined_df.loc[combined_df["Driver"] == driver, "PercentDiff"] = np.mean(
    #         drivers[driver]
    #     )
    # combined_df.to_csv("pace_evaluation.csv")

    # fig = plot_lap_variance_vs_avg(ts_weekend.race.session.drivers)

    fig.show()

    # for i, attr in enumerate(["split1", "split2", "split3", "lap_time"][-1:]):
    #     fig = plot_best_laps(sessions, lap_attr=attr, divs_to_show={3})
    #     fig.show()
    pass
