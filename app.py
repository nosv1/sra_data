import dash
import pandas as pd
from dash import Input, Output, dash, dcc, html
from flask import Flask, render_template
from sqlalchemy import create_engine
from sqlalchemy import text as sql_text
from sqlalchemy.engine import Connection as SQLAConnection
from sqlalchemy.exc import PendingRollbackError

import queries

DATABASE_URI = "mysql+mysqlconnector://SRA:SRA@10.0.0.227/SRA"

SQLAEngine = create_engine(DATABASE_URI)


app = Flask(__name__)

dash_app = dash.Dash(__name__, server=app, url_base_pathname="/dashboard/")


@app.route("/")
def home():
    return render_template("home.html")


@dash_app.callback(
    Output("all-finish-positions-for-car", "figure"),
    Input("car-number-dropdown", "value"),
)
def update_chart(car_number):
    with SQLAEngine.connect() as connection:
        df = queries.get_race_data(connection, car_number)
        fig = {
            "data": [
                {
                    "x": df["session_file"],
                    "y": df["finish_position"],
                    "type": "line",
                    "name": f"Finish Position for Car {car_number}",
                }
            ],
            "layout": {"title": f"Finish Position for Car {car_number}"},
        }
        return fig


@dash_app.callback(
    Output("laps-for-driver", "children"),
    Input("session-file-input", "value"),
    Input("driver-id-input", "value"),
)
def update_laps(session_file, driver_id):
    if session_file and driver_id:
        with SQLAEngine.connect() as connection:
            df = queries.get_laps_for_driver_in_session(
                connection, session_file, driver_id
            )
            return html.Pre(df.to_string())
    return "Enter session file and player ID to see laps."


@dash_app.callback(
    Output("laps-for-driver-graph", "figure"),
    Input("session-file-input", "value"),
    Input("server-number-input", "value"),
    Input("driver-id-input", "value"),
)
def update_lap_line_graph(session_file: str, server_number: int, driver_id: str):
    with SQLAEngine.connect() as connection:
        df = queries.get_laps_for_driver_in_session(
            db_connection=connection,
            session_file=session_file,
            server_number=server_number,
            driver_id=driver_id,
        )
        fig = {
            "data": [
                {
                    "x": df["lap_number"],
                    "y": df["laptime"],
                    "type": "line",
                    "name": f"Lap Time for {driver_id}",
                }
            ],
            "layout": {"title": f"Lap Time for {driver_id} in {session_file}"},
        }
        return fig


@dash_app.callback(
    Output("query-result", "children"),
    Input("query-input", "value"),
)
def execute_query(query):
    if query:
        try:
            with SQLAEngine.connect() as connection:
                df = pd.read_sql_query(sql_text(query), connection)
                return html.Pre(df.to_string())
        except Exception as e:
            return html.Pre(f"Error: {str(e)}")
    return "Enter a query to see results."


with SQLAEngine.connect() as connection:
    dash_app.layout = html.Div(
        [
            html.H1("Race car_results Dashboard"),
            # Finish positions for car
            dcc.Dropdown(
                id="car-number-dropdown",
                options=[
                    {"label": car_number, "value": car_number}
                    for car_number in queries.get_unique_car_numbers(connection)
                ],
                value=518,
                placeholder="Select a car number",
            ),
            dcc.Graph(id="all-finish-positions-for-car"),
            html.Br(),
            # Laps for driver
            html.Div(
                [
                    html.Label("Session File:"),
                    dcc.Input(
                        id="session-file-input",
                        type="text",
                        placeholder="Enter session file",
                    ),
                ]
            ),
            html.Div(
                [
                    html.Label("Driver ID:"),
                    dcc.Input(
                        id="driver-id-input", type="text", placeholder="Enter driver ID"
                    ),
                ]
            ),
            html.Div(
                [
                    html.Label("Server Number:"),
                    dcc.Input(
                        id="server-number-input",
                        type="number",
                        placeholder="Enter server number",
                    ),
                ]
            ),
            html.Div(id="laps-for-driver"),
            html.Br(),
            # Lap time line graph
            dcc.Graph(id="laps-for-driver-graph"),
            html.Br(),
            # SQL Query
            html.Div(
                [
                    html.Label("SQL Query:"),
                    dcc.Textarea(
                        id="query-input",
                        placeholder="Enter your SQL query here",
                        style={"width": "100%", "height": 200},
                    ),
                ]
            ),
            html.Div(id="query-result"),
            html.Br(),
        ]
    )

if __name__ == "__main__":
    app.run(debug=True)
