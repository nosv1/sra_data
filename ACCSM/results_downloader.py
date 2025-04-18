import argparse
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup as bs
from bs4.element import Tag
from pytz import timezone as pytz_timezone

EASTERN_TZ = pytz_timezone("US/Eastern")

current_dir = os.path.dirname(__file__)
sys.path.append(os.path.join(current_dir, ".."))
from threading import Lock

from utils.Database import Neo4jDatabase
from utils.queries import CarModels

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0"
}


def try_get_json(url: str, wait: int = 4, force_wait: bool = False) -> dict:
    total_waited = 0
    while True:
        response = requests.get(url)

        if force_wait:
            time.sleep(wait)  # rate limited 5 requests per 20 seconds

        if response.status_code == 200:
            return response.json()

        elif response.status_code == 429:
            print(
                f"Rate limited. Waiting {wait} seconds (of {total_waited} seconds)..."
            )
            time.sleep(wait)
            total_waited += wait

        else:
            raise Exception(f"Failed to download {url}")


def folder_from_session_type(session_type: str) -> str:
    return {
        "P": "practices",
        "FP": "practices",
        "Q": "qualifyings",
        "Q1": "qualifyings",
        "Q2": "qualifyings",
        "R": "races",
        "R1": "races",
        "R2": "races",
        "R3": "races",
    }.get(session_type, "unknown")


def construct_filename(
    session_type: str, track: str, accsm_file: str, server: str
) -> str:
    folder = folder_from_session_type(session_type)
    return f"{folder}/{track}_{accsm_file}_{server}.json"


def get_accsm_results():
    base_url = lambda server: f"https://accsm{server}.simracingalliance.com"
    results_list_url = (
        lambda server, page: f"{base_url(server)}/api/results/list.json?page={page}&sort=date"
    )
    results_file_url = (
        lambda server, filename: f"{base_url(server)}/results/download/{filename}.json"
    )

    current_dir = os.path.dirname(__file__)
    downloads_dir = os.path.join(current_dir, "downloads")

    servers = [1, 2, 3, 4, 5, 6, 7]

    def find_missing_session(server: int):
        page = 0
        num_pages = float("inf")
        file_exists = False
        while page < num_pages and not file_exists:
            results_list_json = try_get_json(
                results_list_url(server, page), force_wait=True
            )
            num_pages = results_list_json["num_pages"]
            print(f"Server {server} - Page {page}/{num_pages}")

            if not num_pages:
                break

            for result in results_list_json["results"]:
                track = result["track"]
                session_type = result["session_type"]
                finish_time = datetime.fromisoformat(result["date"][:-1]).replace(
                    tzinfo=timezone.utc
                )
                accsm_file = result["results_page_url"].split("/")[-1]

                date_is_before_s11 = finish_time < datetime(
                    2024, 5, 15, tzinfo=timezone.utc
                )
                if session_type == "FP" and date_is_before_s11:
                    print(f"Skipping {track} {session_type} {finish_time} - {server}")
                    continue

                filename = construct_filename(
                    session_type, track, accsm_file, str(server)
                )
                path = os.path.join(downloads_dir, filename)
                if os.path.exists(path):
                    # continue
                    print(f"File {path} already exists... skipping {filename}")
                    file_exists = True
                    break

                file_url = results_file_url(server, accsm_file)
                results_file_json = try_get_json(file_url, force_wait=True)
                with open(path, "w") as f:
                    json.dump(results_file_json, f, indent=4)
                print(f"Downloaded {filename}")

            page += 1

    with ThreadPoolExecutor() as executor:
        executor.map(find_missing_session, servers)
    pass


def parse_sra_result(session_url: str, is_race: bool) -> dict:
    """
    FIXME: didn't handle alternate weather
    FIXME: didn't handle alternate car groups
    FIXME: didn't handle alternate cup categories

    the leaderboard lines have member ids instead of player ids, these need to be matched outside of this function
    """

    # https://www.simracingalliance.com/results/ttserver6/qualifying/250305_150120_Q

    # with open("out.html", "r") as f:
    #     html = bs(f.read(), "html.parser")

    html = bs(requests.get(session_url, headers=HEADERS).content, "html.parser")
    tab_content = html.find("div", {"class": "tab-pane show active"})
    tables = tab_content.find_all("table")
    results_table: Tag = tables[0]
    laps_tables: list[Tag] = tables[1:]
    is_driver_swap_race: Tag = tab_content.find(
        "p", {"class": "fs-5 mt-3 mb-0"}
    ).find_all("span", {"class": "badge bg-success text-dark"})
    is_driver_swap_race = (
        (is_driver_swap_race[-1].text) == "DRIVER SWAP"
        if (is_driver_swap_race and is_race)
        else False
    )

    laps = []
    leaderboard_lines = []

    for r_idx, row in enumerate(results_table.find_all("tr")):
        #  <tr>
        #   <!-- Position -->
        #   <td>
        #    #1
        #    <span style="color:gold" title="Gooooooooooooolddddddd!!!!!1!!!!">
        #     <svg class="bi bi-trophy-fill" fill="currentColor" height="14px" viewbox="0 0 16 16" width="14px" xmlns="http://www.w3.org/2000/svg">
        #      <path d="M2.5.5A.5.5 0 0 1 3 0h10a.5.5 0 0 1 .5.5c0 .538-.012 1.05-.034 1.536a3 3 0 1 1-1.133 5.89c-.79 1.865-1.878 2.777-2.833 3.011v2.173l1.425.356c.194.048.377.135.537.255L13.3 15.1a.5.5 0 0 1-.3.9H3a.5.5 0 0 1-.3-.9l1.838-1.379c.16-.12.343-.207.537-.255L6.5 13.11v-2.173c-.955-.234-2.043-1.146-2.833-3.012a3 3 0 1 1-1.132-5.89A33.076 33.076 0 0 1 2.5.5zm.099 2.54a2 2 0 0 0 .72 3.935c-.333-1.05-.588-2.346-.72-3.935zm10.083 3.935a2 2 0 0 0 .72-3.935c-.133 1.59-.388 2.885-.72 3.935z">
        #      </path>
        #     </svg>
        #    </span>
        #   </td>
        #   <!-- Name -->
        #   <td>
        #    <span class="hidden-md-down badge badge-division-1 me-2">
        #     7
        #    </span>
        #    <i class="flag flag-ca" data-bs-placement="top" data-bs-toggle="tooltip" title="Canada">
        #    </i>
        #    <a href="#qualifying_1001">
        #     David Mustillo
        #    </a>
        #    <a class="sra-gold-hover ms-1" data-bs-placement="top" data-bs-toggle="tooltip" href="https://www.simracingalliance.com/member_stats/?member=f14f75eef2a9634cf41998e42b4d6ca6492b53e07401d4d81f535fd253798d2e" target="_blank" title="View member stats">
        #     <i class="fi fi-chart-up">
        #     </i>
        #    </a>
        #   </td>
        #   <!-- Car -->
        #   <td>
        #    <span>
        #     <img alt="Ford Mustang GT3" class="img-fluid" data-bs-placement="top" data-bs-toggle="tooltip" src="https://static.simracingalliance.com/assets/images/logo/manufacturers/light/ford.png" style="width:26px;margin: 0 6px 0 2px;" title="Ford Mustang GT3"/>
        #    </span>
        #    <span class="hidden-md-down">
        #     <span data-bs-placement="right" data-bs-toggle="tooltip" title="Year: 2024">
        #      Ford Mustang GT3
        #     </span>
        #    </span>
        #    <span class="float-end hidden-md-down">
        #    </span>
        #   </td>
        #   <!-- Best Lap  -->
        #   <td>
        #    <span class="sra-purple-fastest-lap-sector">
        #     1:41.757
        #    </span>
        #   </td>
        #   <!-- Number of Laps -->
        #   <td>
        #    3
        #   </td>
        #  </tr>
        cols: list[Tag] = row.find_all("td")
        if len(cols) < 5:
            continue

        class COLUMNS:
            RACE_NUMBER: int = 1
            NAME: int = 1 if not is_driver_swap_race else 2
            CAR_ID: int = NAME
            MEMBER_URL: int = NAME
            MEMBER_ID: int = NAME
            CAR_MODEL: int = CAR_ID + 1
            TOTAL_TIME: int = CAR_MODEL + 1
            BEST_LAP: int = (TOTAL_TIME + 1) if is_race else (CAR_MODEL + 1)
            AVG_CLEAN_LAP: int = BEST_LAP + 1
            NUM_LAPS: int = (AVG_CLEAN_LAP + 1) if is_race else (BEST_LAP + 1)

        race_number = cols[COLUMNS.RACE_NUMBER].find("span").text.strip()
        name = cols[COLUMNS.NAME].find("a").text.strip()
        first_name = name.split()[0] if name else ""
        last_name = " ".join(name.split()[1:]) if name else ""
        car_id = cols[COLUMNS.CAR_ID].find("a")["href"].split("_")[-1]
        member_url = cols[COLUMNS.MEMBER_URL].find("a", {"class": "sra-gold-hover"})[
            "href"
        ]
        member_id = member_url.split("/")[-1].split("member=")[-1]
        car_model = [
            cm.text.strip()
            for cm in cols[COLUMNS.CAR_MODEL].find_all("span")
            if cm.text.strip()
        ][0]
        car_model = CarModels.model_name_dict[
            CarModels.sra_corrections.get(car_model, car_model)
        ]
        best_lap = cols[COLUMNS.BEST_LAP].text.strip()
        num_laps = int(cols[COLUMNS.NUM_LAPS].text.strip())

        total_time = "0:00.000"
        if is_race:
            total_time = cols[COLUMNS.TOTAL_TIME].text.strip()
            avg_clean_lap = cols[COLUMNS.AVG_CLEAN_LAP].text.strip()

        def time_to_milli(lap: str) -> int:
            # lap needs to be "m:ss.000"
            # it comes in as "m:ss.xxxINV" or "m:ss.xxx +0.000 or "00.000"
            lap = lap.split()[0]
            lap = re.sub(r"[^0-9:.]", "", lap)
            hours = 0
            minutes = 0
            seconds = 0
            if ":" in lap:
                parts = lap.split(":")
                seconds = float(parts[-1])
                minutes = int(parts[-2])
                if len(parts) > 2:
                    hours = int(parts[-3])
            else:
                seconds = float(lap)
            minutes += hours * 60
            seconds += minutes * 60
            milliseconds = seconds * 1000
            return int(milliseconds)

        leaderboard_line = {
            "car": {
                "carId": int(car_id),
                "carModel": car_model,
                "carGroup": "GT3",
                "carGuid": -1,
                "teamGuid": -1,
                "cupCategory": -1,
                "drivers": [
                    {
                        "firstName": first_name,
                        "lastName": last_name,
                        "playerId": "PlayerID",  # Placeholder, sra results don't have player ids, but they do have member ids, these can be matched later
                        "memberId": member_id,
                        "shortName": name,
                    }
                ],
                "nationality": -1,
                "raceNumber": int(race_number),
                "teamName": "",
            },
            "currentDriver": {
                "firstName": first_name,
                "lastName": last_name,
                "playerId": "PlayerID",  # Placeholder, sra results don't have player ids, but they do have member ids, these can be matched later
                "memberId": member_id,
                "shortName": "",
            },
            "currentDriverIndex": 0,
            "driverTotalTimes": [],
            "missingMandatoryPitstop": -1,
            "timing": {
                "bestLap": time_to_milli(best_lap) if best_lap != "-" else 0,
                "bestSplits": [],  # Placeholder
                "lapCount": num_laps,
                "lastLap": 0,
                "lastSplitId": 0,
                "lastSplits": [],  # Placeholder
                "totalTime": time_to_milli(total_time) if total_time != "-" else 0,
            },
        }
        leaderboard_lines.append(leaderboard_line)

    best_lap = sys.maxsize
    best_splits = [sys.maxsize, sys.maxsize, sys.maxsize]

    for d_idx, driver_laps in enumerate(laps_tables):

        driver_best_lap = sys.maxsize
        driver_best_splits = [sys.maxsize] * 3
        for r_idx, row in enumerate(driver_laps.find_all("tr")):
            #   <tr>
            #    <!-- Lap -->
            #    <td>
            #     #1
            #    </td>
            #    <!-- Name -->
            #    <td>
            #     David Mustillo
            #    </td>
            #    <!-- Lap Time -->
            #    <td>
            #     <span class="text-gray-600">
            #      1:42.150
            #     </span>
            #     <span class="badge bg-warning text-dark ms-2" data-bs-placement="top" data-bs-toggle="tooltip" title="Invalid">
            #      INV
            #     </span>
            #    </td>
            #    <!-- Sector 1 -->
            #    <td class="hidden-md-down">
            #     <span class="text-gray-600">
            #      21.807
            #     </span>
            #    </td>
            #    <!-- Sector 2 -->
            #    <td class="hidden-md-down">
            #     <span class="text-gray-600">
            #      32.795
            #     </span>
            #    </td>
            #    <!-- Sector 3  -->
            #    <td class="hidden-md-down">
            #     <span class="text-gray-600">
            #      47.547
            #     </span>
            #    </td>
            #   </tr>
            row: Tag
            cols = row.find_all("td")
            if len(cols) < 6:
                continue

            lap = {
                "carId": leaderboard_lines[d_idx]["car"]["carId"],
                "driverIndex": 0,
                "isValidForBest": "INV" not in cols[2].text,
                "laptime": time_to_milli(cols[2].text),
                "splits": [
                    time_to_milli(cols[3].text),
                    time_to_milli(cols[4].text),
                    time_to_milli(cols[5].text),
                ],
            }
            laps.append(lap)

            if lap["isValidForBest"]:
                driver_best_lap = min(driver_best_lap, lap["laptime"])
                driver_best_splits = [
                    min(driver_best_splits[i], lap["splits"][i]) for i in range(3)
                ]
                leaderboard_lines[d_idx]["timing"]["bestSplits"] = driver_best_splits

                best_lap = min(best_lap, lap["laptime"])
                best_splits = [min(best_splits[i], lap["splits"][i]) for i in range(3)]
                pass
            pass
        pass

    session_result = {
        "bestSplits": best_splits if best_lap != sys.maxsize else [sys.maxsize] * 3,
        "bestlap": best_lap if best_lap != sys.maxsize else sys.maxsize,
        "isWetSession": 0,
        "leaderBoardLines": leaderboard_lines,
        "type": 0,
    }

    result_json = {
        "laps": laps,
        "penalties": [],
        "post_race_penalties": [],
        "sessionIndex": 1,
        "raceWeekendIndex": 0,
        "sessionResult": session_result,
        "sessionType": tab_content.find("span", {"class": "badge"}).text.strip(),
        "trackName": str(tab_content)
        .split("Track:")[-1]
        .split("</p>")[0]
        .split(">")[-1]
        .strip()
        .lower()
        .replace(" ", "_"),
        "serverName": tab_content.find("span", {"class": "sra-gold"})
        .text.strip()
        .split("\n")[-1]
        .strip()[1:],
        "metaData": "",
        "Date": "placeholder",  # Placeholder
        "SessionFile": "placeholder",  # Placeholder
    }

    return result_json


def get_member_ids() -> dict:
    neo_driver, neo_session = Neo4jDatabase.connect_database("SRA")
    print(f"Getting member ids...", end="")
    result = neo_driver.execute_query(
        f"""
        MATCH (d:Driver)
        RETURN d.member_id, d.driver_id"""
    )
    print(f"received {len(result.records)} records.")
    Neo4jDatabase.close_connection(neo_driver, neo_session)
    member_ids = {
        record["d.member_id"]: record["d.driver_id"] for record in result.records
    }
    return member_ids


def get_sra_results(
    after_date: datetime = datetime.min.replace(tzinfo=EASTERN_TZ),
    before_date: datetime = datetime.max.replace(tzinfo=EASTERN_TZ),
):
    current_dir = os.path.dirname(__file__)
    downloads_dir = os.path.join(current_dir, "downloads")

    member_ids = get_member_ids()

    servers = ["server1", "server2", "server3", "server4", "server5", "server7"][:3]
    missing_sessions: list[Session] = []
    missing_sessions_lock = Lock()

    class Session:
        def __init__(
            self,
            session_url: str,
            session_type: str,
            finish_time: datetime,
            accsm_file: str,
            path: str,
            filename: str,
        ):
            self.session_url = session_url
            self.session_type = session_type
            self.finish_time = finish_time
            self.accsm_file = accsm_file
            self.path = path
            self.filename = filename
            pass

        def get_missing_session(self):
            result_json = parse_sra_result(
                self.session_url, is_race=self.session_type.startswith("R")
            )
            result_json["Date"] = (
                f"{self.finish_time.replace(tzinfo=None).isoformat()}Z"
            )
            result_json["SessionFile"] = self.accsm_file
            for line in result_json["sessionResult"]["leaderBoardLines"]:
                for driver in line["car"]["drivers"]:
                    member_id = driver["memberId"]
                    driver_id = member_ids.get(member_id, member_id)
                    if driver_id:
                        driver["playerId"] = driver_id
                current_driver = line["currentDriver"]
                member_id = current_driver["memberId"]
                driver_id = member_ids.get(member_id, member_id)
                if driver_id:
                    current_driver["playerId"] = driver_id
            with open(self.path, "w") as f:
                json.dump(result_json, f, indent=4)
            print(f"Downloaded {self.filename}")

    def find_missing_sessions(server: str):
        print(f"Processing server {server}...")
        base_url = f"https://www.simracingalliance.com/results/{server}"
        html = bs(requests.get(base_url, headers=HEADERS).content, "html.parser")
        table = html.find("table", {"id": "resultsTable"})
        for row in table.find_all("tr"):
            row: Tag
            cols: list[Tag] = row.find_all("td")
            if len(cols) < 7:
                continue

            date_str = cols[1].text.strip()
            session_type = cols[2].text.strip()
            session_name = cols[3].find("a").text.strip()
            session_url = cols[3].find("a")["href"]

            finish_time = EASTERN_TZ.localize(
                datetime.strptime(date_str, "%a. %b %d, %Y - %I:%M %p")
            )
            if not (before_date >= finish_time >= after_date):
                break

            accsm_file = session_url.split("/")[-1]
            filename = construct_filename(session_type[0], "", accsm_file, server)
            path = os.path.join(downloads_dir, filename)

            if session_type[0] not in ["R"]:
                continue

            if os.path.exists(path):
                print(f"File {path} already exists... skipping {filename}")
                continue

            print(f"Missing file: {filename}")
            with missing_sessions_lock:
                session = Session(
                    session_url=session_url,
                    session_type=session_type,
                    finish_time=finish_time,
                    accsm_file=accsm_file,
                    path=path,
                    filename=filename,
                )
                missing_sessions.append(session)
            pass
        pass

    with ThreadPoolExecutor() as executor:
        executor.map(find_missing_sessions, servers)

    for session in missing_sessions:
        session: Session
        session.get_missing_session()
    # with ThreadPoolExecutor() as executor:
    # executor.map(lambda s: s.get_missing_session(), missing_sessions)

    pass


def main(argv):
    parser = argparse.ArgumentParser(description="Download and parse racing results.")
    parser.add_argument("--accsm", action="store_true", help="Download ACCSM results")
    parser.add_argument(
        "--sra", action="store_true", help="Download and parse SRA results"
    )
    parser.add_argument(
        "--after-date",
        type=lambda s: EASTERN_TZ.localize(datetime.strptime(s, "%Y-%m-%d")),
        default=datetime.min.replace(tzinfo=EASTERN_TZ),
        help="Filter results after this date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--before-date",
        type=lambda s: EASTERN_TZ.localize(datetime.strptime(s, "%Y-%m-%d")),
        default=datetime.max.replace(tzinfo=EASTERN_TZ),
        help="Filter results before this date (YYYY-MM-DD)",
    )

    args = parser.parse_args(argv)

    if args.accsm:
        get_accsm_results()
    elif args.sra:
        get_sra_results(before_date=args.before_date, after_date=args.after_date)
    else:
        get_accsm_results()
        # get_sra_results(
        #     after_date=EASTERN_TZ.localize(datetime(2025, 4, 8)),
        #     # before_date=EASTERN_TZ.localize(datetime(2025, 3, 29)),
        # )


if __name__ == "__main__":
    main(sys.argv[1:])
