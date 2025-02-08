"""
accsm stores steamids
sra has member ids
you can overlap the race results and match the car numbers to match the
member ids with steam ids
we'll do this only for team series races
"""

# join car_results and driver_sessions and team_series_sessions

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum

import requests
from bs4 import BeautifulSoup as bs4
from bs4.element import ResultSet, Tag
from neo4j import Driver as Neo4jDriver
from neo4j import Session as Neo4jSession

from Database import MySqlDatabase, Neo4jDatabase


@dataclass
class TeamSeriesDriver:
    finish_position: int
    car_number: str
    first_name: str
    last_name: str
    driver_id: str


def get_driver_from_team_series_session_sql(
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


def get_drivers_from_team_series_session_neo4j(
    sra_neo_driver: Neo4jDriver, session_key: str
) -> list[TeamSeriesDriver]:
    sra_neo_results = sra_neo_driver.execute_query(
        f"""
        MATCH (d:Driver)-[]->(c:Car)-[]->(s:Session)
        WHERE 
            TRUE
            AND s.key_ = '{session_key}'
        WITH DISTINCT d, c, s
        RETURN d, c
        ORDER BY c.finish_position
        """
    )

    team_series_drivers: list[TeamSeriesDriver] = []
    for record in sra_neo_results.records:
        driver, car = record.values()
        team_series_drivers.append(
            TeamSeriesDriver(
                finish_position=int(car["finish_position"]),
                car_number=int(car["car_number"]),
                first_name=driver["first_name"],
                last_name=driver["last_name"],
                driver_id=driver["driver_id"],
            )
        )

    return team_series_drivers


@dataclass
class SRARaceDriver:
    finish_position: int
    car_number: int
    first_name: str
    last_name: str
    member_id: str

    def __str__(self):
        return f"{self.finish_position} {self.car_number} {self.first_name} {self.last_name} {self.member_id}"


def get_session_from_sra(session_file, server_number) -> list[SRARaceDriver]:
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

    drivers: list[SRARaceDriver] = []
    for i, row in enumerate(rows[1:]):
        cols = row.find_all("td")
        finish_position = i + 1
        car_number = int(cols[1].find("span", class_="me-2").text.strip())
        driver_name = cols[-6].find("a").text.strip()
        first_name, *last_name = driver_name.split(" ")
        last_name = " ".join(last_name)
        member_id = cols[-6].find("a", class_="ms-1").attrs["href"].split("member=")[1]
        drivers.append(
            SRARaceDriver(
                finish_position=finish_position,
                car_number=car_number,
                first_name=first_name,
                last_name=last_name,
                member_id=member_id,
            )
        )

    return drivers


@dataclass
class Session:
    session_file: str
    server_number: str
    key_: str


def get_race_sessions_neo4j(
    sra_neo_driver: Neo4jDriver, after_date: datetime = datetime.min
) -> list[Session]:

    sra_neo_results = sra_neo_driver.execute_query(
        f"""
        MATCH (s:Session)
        WHERE 
            TRUE
            AND s.server_name CONTAINS "GT3 Team Championship" 
            AND s.session_type CONTAINS "R"
            AND s.finish_time > datetime('{after_date.isoformat()}')
        RETURN s.session_file, s.server_number, s.key_
        """
    )

    sessions: list[Session] = []
    for record in sra_neo_results.records:
        session_file, server_number, key_ = record.values()
        sessions.append(Session(session_file, server_number, key_))

    return sessions


def match_sra_id_steam_id_neo4j(
    neo_driver: Neo4jDriver,
    sra_drivers: list[SRARaceDriver],
    team_series_drivers: list[TeamSeriesDriver],
):
    for tsd, sra_driver in zip(team_series_drivers, sra_drivers):
        if tsd.car_number != sra_driver.car_number:
            continue
        if tsd.finish_position != sra_driver.finish_position:
            continue

        # Check if the match already exists
        existing_match = neo_driver.execute_query(
            f"""
            MATCH (d:Driver)
            WHERE 
            TRUE
                AND d.first_name = '{Neo4jDatabase.handle_bad_string(tsd.first_name)}'
                AND d.last_name = '{Neo4jDatabase.handle_bad_string(tsd.last_name)}'
                AND d.member_id = '{sra_driver.member_id}'
            RETURN d
            """
        )

        if existing_match.records:
            print(
                f"Match already exists for {tsd.first_name} {tsd.last_name} with {sra_driver.member_id}"
            )
            continue

        print(f"Matching {tsd.first_name} {tsd.last_name} with {sra_driver.member_id}")

        sra_neo_results = neo_driver.execute_query(
            f"""
            MATCH (d:Driver)
            WHERE 
                TRUE
                AND d.first_name = '{Neo4jDatabase.handle_bad_string(tsd.first_name)}'
                AND d.last_name = '{Neo4jDatabase.handle_bad_string(tsd.last_name)}'
            SET 
                d.member_id = '{sra_driver.member_id}', 
                d.first_name = '{Neo4jDatabase.handle_bad_string(sra_driver.first_name)}',
                d.last_name = '{Neo4jDatabase.handle_bad_string(sra_driver.last_name)}'
            """
        )

    return


class Division(Enum):
    NOT_RATED = None
    SRALIEN = 1.0
    DIVISION_1_GOLD = 1.0
    DIVISION_1_SILVER = 1.5
    DIVISION_2_GOLD = 2.0
    DIVISION_2_SILVER = 2.5
    DIVISION_3_GOLD = 3.0
    DIVISION_3_SILVER = 3.5
    DIVISION_4_GOLD = 4.0
    DIVISION_4_SILVER = 4.5
    DIVISION_5_GOLD = 5.0
    DIVISION_5_SILVER = 5.5
    DIVISION_6_GOLD = 6.0
    DIVISION_6_SILVER = 6.5
    DIVISION_7_GOLD = 7.0
    DIVISION_7_SILVER = 7.5


@dataclass
class SRARatedDriver:
    division: Division
    driver_name: str
    member_id: str
    rating: float

    @property
    def first_name(self):
        return self.driver_name.split(" ")[0]

    @property
    def last_name(self):
        return " ".join(self.driver_name.split(" ")[1:])


def get_sra_drivers() -> list[SRARatedDriver]:
    sra_drivers_url = "https://www.simracingalliance.com/about/drivers"

    # <tr>
    #     <!-- Number / Name -->
    #     <td><span class="float-end"><img class="img-fluid discord-role-badge discord-role-badge-36" src="https://static.simracingalliance.com/assets/images/roles/Division 1 Gold.png" data-bs-toggle="tooltip" data-bs-placement="top" alt="Division 1 Gold" aria-label="Division 1 Gold" data-bs-original-title="Division 1 Gold"></span><span class="badge badge-division-1 me-2">1</span><i class="flag flag-co" data-bs-toggle="tooltip" data-bs-placement="top" aria-label="Colombia" data-bs-original-title="Colombia"></i> <a class="sra-gold" href="https://www.simracingalliance.com/member_stats/?member=c2451e5e42a80b3a1ca3a2eeb8a1618de26edc78fd956e179f22dcb3172d1d13">Estefano Barrera</a> <span data-bs-toggle="tooltip" data-bs-placement="top" data-bs-original-title="GT3 Team Series Drivers Champion">🏆</span></td>
    #     <!-- SRAting -->
    #     <td><span class="float-end" data-bs-html="true" data-bs-toggle="tooltip" data-bs-placement="top" data-bs-original-title="Pace: 99.74<br />Perf.: 95.70<br />Safety: 99.29"><a class="sra-gold" href="https://www.simracingalliance.com/about/srating/?member=c2451e5e42a80b3a1ca3a2eeb8a1618de26edc78fd956e179f22dcb3172d1d13">98.94</a></span></td>
    #     <!-- # Races -->
    #     <td><span class="float-end">18</span></td>
    # </tr>

    response = requests.get(sra_drivers_url)
    soup = bs4(response.content, "html.parser")
    tables = soup.find_all("table", class_="table table-responsive table-drivers")
    rows = [row for table in tables[:2] for row in table.find_all("tr")]

    sra_rated_drivers: list[SRARatedDriver] = []

    for row in rows:
        cols = row.find_all("td")
        if not cols:
            continue
        division_badge = (
            cols[0].find("span", class_="float-end").find("img").attrs["alt"]
        )
        div_key = division_badge.replace(" ", "_").upper()
        if div_key == "DRIVER_NOT_RATED":
            division = Division.NOT_RATED
        else:
            division = Division[div_key]
        driver_name = cols[0].find("a").text
        member_id = cols[0].find("a").attrs["href"].split("member=")[1]
        rating = cols[1].find("a").text
        if not rating:
            rating = 0.0

        sra_rated_drivers.append(
            SRARatedDriver(
                division=division,
                driver_name=driver_name,
                member_id=member_id,
                rating=rating,
            )
        )

    return sra_rated_drivers


def update_driver_ratings_neo4j(
    neo_driver: Neo4jDriver, sra_rated_drivers: list[SRARatedDriver]
):
    for sra_driver in sra_rated_drivers:
        print(f"Updating {sra_driver.driver_name}...")
        neo_driver.execute_query(
            f"""
            MATCH (d:Driver)
            WHERE d.member_id = '{sra_driver.member_id}'
                OR (d.member_id IS NULL
                    AND d.first_name = '{Neo4jDatabase.handle_bad_string(sra_driver.first_name)}' 
                    AND d.last_name = '{Neo4jDatabase.handle_bad_string(sra_driver.last_name)}'
                )
            SET 
                d.first_name = '{Neo4jDatabase.handle_bad_string(sra_driver.first_name)}',
                d.last_name = '{Neo4jDatabase.handle_bad_string(sra_driver.last_name)}',
                d.rating = {float(sra_driver.rating)} 
                {f', d.division = {float(sra_driver.division.value)}' if sra_driver.division != Division.NOT_RATED else ''}
            """
        )

    return


@dataclass
class SRAQualiDriver:
    driver_name: str
    driver_id: str
    avg_lap: str


def get_hot_stint_quali_drivers():
    hot_stint_quali_url = "https://www.simracingalliance.com/leaderboards/hot_stint_qualifying/?unique_drivers=1"

    # <tr class="Division-1">
    #     <!-- Rank -->
    #     <td><a class="leaderboard-rank" name="rank-1"></a>#1 <span style="color:gold" title="Gooooooooooooolddddddd!!!!!1!!!!"><svg width="14px" height="14px" xmlns="http://www.w3.org/2000/svg" fill="currentColor" class="bi bi-trophy-fill" viewBox="0 0 16 16"><path d="M2.5.5A.5.5 0 0 1 3 0h10a.5.5 0 0 1 .5.5c0 .538-.012 1.05-.034 1.536a3 3 0 1 1-1.133 5.89c-.79 1.865-1.878 2.777-2.833 3.011v2.173l1.425.356c.194.048.377.135.537.255L13.3 15.1a.5.5 0 0 1-.3.9H3a.5.5 0 0 1-.3-.9l1.838-1.379c.16-.12.343-.207.537-.255L6.5 13.11v-2.173c-.955-.234-2.043-1.146-2.833-3.012a3 3 0 1 1-1.132-5.89A33.076 33.076 0 0 1 2.5.5zm.099 2.54a2 2 0 0 0 .72 3.935c-.333-1.05-.588-2.346-.72-3.935zm10.083 3.935a2 2 0 0 0 .72-3.935c-.133 1.59-.388 2.885-.72 3.935z"></path></svg></span></td>
    #     <!-- Filtered Rank -->
    #     <td class="filtered-rank sra-gold" style="display:none;"></td>
    #     <!-- Name -->
    #                                                     <td><span class="float-end hidden-md-up" style="vertical-align: top;"><img class="img-fluid" style="width:30px;margin: 0;" src="https://static.simracingalliance.com/assets/images/logo/manufacturers/light/nissan.png" alt="Nissan GT-R Nismo GT3" data-bs-toggle="tooltip" data-bs-placement="left" aria-label="Nissan GT-R Nismo GT3" data-bs-original-title="Nissan GT-R Nismo GT3"></span><span class="float-end"><img class="img-fluid discord-role-badge discord-role-badge-22 me-2 hidden-md-down" src="https://static.simracingalliance.com/assets/images/roles/Alien.png" data-bs-toggle="tooltip" data-bs-placement="left" aria-label="Reference lap time: Alien" data-bs-original-title="Reference lap time: Alien"><img class="img-fluid discord-role-badge discord-role-badge-36 me-2" src="https://static.simracingalliance.com/assets/images/roles/SRAlien.png" data-bs-toggle="tooltip" data-bs-placement="left" aria-label="SRAlien" data-bs-original-title="SRAlien"></span><span class="hidden-md-down badge badge-sralien me-2">434</span><i class="flag flag-it" data-bs-toggle="tooltip" data-bs-placement="top" aria-label="Italy" data-bs-original-title="Italy"></i> <a class="sra-gold-hover" href="https://www.simracingalliance.com/member_leaderboards/hot_stint_qualifying/?member=3360bde2e69104c1dd7f7c8bacb548ff3798bd5c082efea184bc84e740541500">Jacob Palmieri</a></td>

    #     <!-- Car -->
    #     <td class="hidden-md-down">
    #         <span>
    #             <img class="img-fluid" style="width:26px;margin: 0 6px 0 2px;" src="https://static.simracingalliance.com/assets/images/logo/manufacturers/light/nissan.png" alt="Nissan GT-R Nismo GT3">
    #         </span>
    #         <span data-bs-toggle="tooltip" data-bs-placement="right" data-bs-original-title="Year: 2018">Nissan GT-R Nismo GT3</span>
    #                                                         </td>

    #     <!-- Lap Time -->
    #     <td><span class="sra-purple-fastest-lap-sector">1:57.157</span>                                                </td>

    #                                                     <!-- Sector 1 -->
    #     <td class="hidden-md-down">34.601</td>

    #     <!-- Sector 2 -->
    #     <td class="hidden-md-down">49.285</td>

    #     <!-- Sector 3 -->
    #     <td class="hidden-md-down"><span class="sra-purple-fastest-lap-sector">33.269</span></td>

    #     <!-- Date -->
    #     <td class="hidden-md-down"><span data-bs-toggle="tooltip" data-bs-placement="right" aria-label="Fri. November 22, 2024 1:08 AM EST" data-bs-original-title="Fri. November 22, 2024 1:08 AM EST"><svg width="18px" height="18px" style="position:relative;top:-2px;" xmlns="http://www.w3.org/2000/svg" fill="currentColor" class="bi bi-calendar-week" viewBox="0 0 16 16"><path d="M11 6.5a.5.5 0 0 1 .5-.5h1a.5.5 0 0 1 .5.5v1a.5.5 0 0 1-.5.5h-1a.5.5 0 0 1-.5-.5v-1zm-3 0a.5.5 0 0 1 .5-.5h1a.5.5 0 0 1 .5.5v1a.5.5 0 0 1-.5.5h-1a.5.5 0 0 1-.5-.5v-1zm-5 3a.5.5 0 0 1 .5-.5h1a.5.5 0 0 1 .5.5v1a.5.5 0 0 1-.5.5h-1a.5.5 0 0 1-.5-.5v-1zm3 0a.5.5 0 0 1 .5-.5h1a.5.5 0 0 1 .5.5v1a.5.5 0 0 1-.5.5h-1a.5.5 0 0 1-.5-.5v-1z"></path><path d="M3.5 0a.5.5 0 0 1 .5.5V1h8V.5a.5.5 0 0 1 1 0V1h1a2 2 0 0 1 2 2v11a2 2 0 0 1-2 2H2a2 2 0 0 1-2-2V3a2 2 0 0 1 2-2h1V.5a.5.5 0 0 1 .5-.5zM1 4v10a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1V4H1z"></path></svg></span></td>

    # </tr>

    response = requests.get(hot_stint_quali_url)
    soup = bs4(response.content, "html.parser")
    tables = soup.find_all("table", class_="table table-responsive table-leaderboard")
    rows = [row for table in tables for row in table.find_all("tr")]

    hot_stint_quali_drivers: list[SRAQualiDriver] = []

    for row in rows:
        cols = row.find_all("td")
        if not cols:
            continue
        driver_name = str(cols[2].find("a").text).strip()
        driver_id = str(cols[2].find("a").attrs["href"].split("member=")[1]).strip()
        avg_lap = str(cols[4].text).strip()[:8]

        print(f"Fetched {driver_name} - {avg_lap}")
        hot_stint_quali_drivers.append(
            SRAQualiDriver(
                driver_name=driver_name,
                driver_id=driver_id,
                avg_lap=avg_lap,
            )
        )

    return hot_stint_quali_drivers


def set_hot_stint_quali_times_neo4j(
    neo_driver: Neo4jDriver, hot_stint_quali_drivers: list[SRAQualiDriver]
):
    for driver in hot_stint_quali_drivers:
        print(f"Setting {driver.driver_name} - {driver.avg_lap}")
        neo_driver.execute_query(
            f"""
            MATCH (d:Driver)
            WHERE 
                TRUE
                AND d.member_id = '{driver.driver_id}'
            SET d.hot_stint_quali_time = '{driver.avg_lap}'
            """
        )

    return


if __name__ == "__main__":
    neo_driver, neo_session = Neo4jDatabase.connect_database("SRA")

    # hot_stint_quali_drivers = get_hot_stint_quali_drivers()
    # set_hot_stint_quali_times_neo4j(neo_driver, hot_stint_quali_drivers)

    ## sync member ids with steam ids
    for session in get_race_sessions_neo4j(
        neo_driver, after_date=datetime.now() - timedelta(days=7)
    ):
        print(f"Processing {session.session_file} - {session.server_number}")
        team_series_drivers = get_drivers_from_team_series_session_neo4j(
            neo_driver, session.key_
        )
        sra_drivers = get_session_from_sra(session.session_file, session.server_number)
        match_sra_id_steam_id_neo4j(neo_driver, sra_drivers, team_series_drivers)

    ## update driver ratings and divisions
    update_driver_ratings_neo4j(neo_driver, get_sra_drivers())

    Neo4jDatabase.close_connection(neo_driver, neo_session)
    exit()
