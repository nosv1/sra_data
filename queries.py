import pandas as pd
from sqlalchemy import text as sql_text
from sqlalchemy.engine import Connection as SQLAConnection


def get_unique_car_numbers(db_connection: SQLAConnection) -> list:
    query = """
    SELECT DISTINCT car_number FROM car_results ORDER BY car_number ASC;
    """
    df = pd.read_sql_query(sql_text(query), db_connection)
    return df["car_number"].tolist()


def get_unique_server_names(db_connection: SQLAConnection) -> list:
    query = """
    SELECT DISTINCT server_name 
    FROM sessions 
    WHERE server_name IS NOT NULL 
        AND server_name <> '' 
    ORDER BY server_name ASC;
    """
    df = pd.read_sql_query(sql_text(query), db_connection)
    return df["server_name"].tolist()


def get_laps_for_driver_in_session(
    db_connection: SQLAConnection, session_file: str, server_number: int, driver_id: str
) -> pd.DataFrame:
    query = f"""
    SELECT * 
    FROM car_laps 
    WHERE session_file = '{session_file}'
        AND server_number = '{server_number}'
        AND driver_id = '{driver_id}'
    ORDER BY lap_number ASC;
    """
    df = pd.read_sql_query(sql_text(query), db_connection)
    return df


def get_drivers_in_session(
    db_connection: SQLAConnection, session_file: str, server_number: int
) -> pd.DataFrame:
    query = f"""
    SELECT DISTINCT driver_id 
    FROM car_laps 
    WHERE session_file = '{session_file}' 
        AND server_number = {server_number};
    """
    df = pd.read_sql_query(sql_text(query), db_connection)
    return df


def get_race_data(db_connection: SQLAConnection, car_number: int) -> pd.DataFrame:
    query = f"""
    SELECT * FROM car_results WHERE CAR_NUMBER = {car_number};
    """
    df = pd.read_sql_query(sql_text(query), db_connection)
    return df
