"""
accsm stores steamids
sra has member ids
you can overlap the race results and match the car numbers to match the
member ids with steam ids
we'll do this only for team series races
"""

# join car_results and driver_sessions and team_series_sessions

from dataclasses import dataclass

import requests
from bs4 import BeautifulSoup as bs4
from bs4.element import ResultSet, Tag

from Database import MySqlDatabase


@dataclass
class TeamSeriesDriver:
    finish_position: int
    car_number: str
    first_name: str
    last_name: str
    driver_id: str


def get_driver_from_team_series_session(
    session_file, server_number
) -> list[TeamSeriesDriver]:
    sra_db, sra_db_cursor = MySqlDatabase.connect_database("SRA", verbose=False)
    sra_db_cursor.execute(
        f"""
        SELECT tsd.finish_position, tsd.car_number, tsd.first_name, tsd.last_name, tsd.driver_id
        FROM team_series_drivers tsd
        WHERE tsd.session_file = '{session_file}' AND tsd.server_number = '{server_number}'
        """
    )
    print(f"Fetching team series drivers: {session_file} - {server_number}...")
    sra_db_results = sra_db_cursor.fetchall()
    MySqlDatabase.close_connection(sra_db, sra_db_cursor, verbose=False)

    drivers: list[TeamSeriesDriver] = []
    for result in sra_db_results:
        drivers.append(TeamSeriesDriver(*result))

    return drivers


@dataclass
class SRADriver:
    finish_position: int
    car_number: int
    first_name: str
    last_name: str
    member_id: str

    def __str__(self):
        return f"{self.finish_position} {self.car_number} {self.first_name} {self.last_name} {self.member_id}"


def get_session_from_sra(session_file, server_number) -> list[SRADriver]:
    url = f"https://www.simracingalliance.com/results/server{server_number}/race/{session_file}"

    print(f"Fetching SRA drivers: {session_file} - {server_number}...")
    response = requests.get(url)
    soup = bs4(response.content, "html.parser")

    h3_headers: ResultSet[Tag] = soup.find_all("h3")
    # not sure why find('h3', text='Race Results') doesn't work
    race_results_header: Tag = None
    for header in h3_headers:
        if header.text == "Race Results":
            race_results_header = header
            break
    results_table = race_results_header.find_next("table", class_="table-results")
    rows = results_table.find_all("tr")

    drivers: list[SRADriver] = []
    for i, row in enumerate(rows[1:]):
        cols = row.find_all("td")
        finish_position = i + 1
        car_number = int(cols[1].find("span", class_="me-2").text.strip())
        driver_name = cols[1].find("a").text.strip()
        first_name, *last_name = driver_name.split(" ")
        last_name = " ".join(last_name)
        member_id = cols[1].find("a", class_="ms-1").attrs["href"].split("member=")[1]
        drivers.append(
            SRADriver(
                finish_position=finish_position,
                car_number=car_number,
                first_name=first_name,
                last_name=last_name,
                member_id=member_id,
            )
        )

    return drivers


def match_sra_id_steam_id(
    sra_drivers: list[SRADriver], team_series_drivers: list[TeamSeriesDriver]
):

    sra_db_connection, sra_db_cursor = MySqlDatabase.connect_database(
        "SRA", verbose=False
    )

    try:
        for tsd, sra_driver in zip(team_series_drivers, sra_drivers):
            if tsd.car_number != sra_driver.car_number:
                continue
            if tsd.finish_position != sra_driver.finish_position:
                continue

            sra_db_cursor.execute(
                f"""
                UPDATE drivers 
                SET member_id = '{sra_driver.member_id}' 
                WHERE driver_id = '{tsd.driver_id}'
                """
            )

        sra_db_connection.commit()
    finally:
        MySqlDatabase.close_connection(sra_db_connection, sra_db_cursor, verbose=False)


def get_race_sessions() -> list[str]:
    sra_db_connection, sra_db_cursor = MySqlDatabase.connect_database(
        "SRA", verbose=False
    )

    sra_db_cursor.execute(
        # AND session_file <= '{start_at_session}'
        f"""
        SELECT session_file, server_number
        FROM team_series_sessions
        WHERE session_type = 'R'
        ORDER BY session_file DESC
        """
    )

    session_files = sra_db_cursor.fetchall()

    MySqlDatabase.close_connection(sra_db_connection, sra_db_cursor, verbose=False)

    return session_files


if __name__ == "__main__":
    # start_at_session = "231107_224927_R"
    for session_file, server_number in get_race_sessions():
        team_series_drivers = get_driver_from_team_series_session(
            session_file, server_number
        )
        try:
            sra_drivers = get_session_from_sra(session_file, server_number)
        except AttributeError:
            print(f"Error fetching {session_file} - {server_number}")
        match_sra_id_steam_id(sra_drivers, team_series_drivers)
