import os
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup as bs4
from bs4.element import Tag


def download_file(url: str, filepath: os.path):
    response = requests.get(url)
    if response.status_code == 200:
        with open(filepath, "wb") as f:
            f.write(response.content)
    return response.status_code


if __name__ == "__main__":

    # as of 10/23/24, obv could just check to see if page returns 404
    server_nums = [1, 2, 3, 4, 5, 6, 7, 8]
    team_series_servers = {1, 2, 3, 4}
    mcm_liaw_servers = {5, 6}  # multi-class madness and league in a week
    endurance_series_servers = {7, 8}
    max_pages = [359, 401, 324, 145, 130, 0, 143, 6]
    drivers_to_find = ["Arct"]
    tracks_to_find = []
    start_date = datetime(2024, 5, 19)
    end_date = datetime(2024, 7, 24)
    tracks_to_find = [
        "barcelona",
        "imola",
    ]
    query = "S76561198279907335"

    for server_num in server_nums:
        page_num = 0
        while True:
            print(f"Server {server_num} - Page {page_num}")

            # url template https://accsm3.simracingalliance.com/results?page=324
            url = f"https://accsm{server_num}.simracingalliance.com/results?page={page_num}"
            # url = f"https://accsm{server_num}.simracingalliance.com/results?page={page_num}&q={query}"

            ###  example row
            # <tr class="row-link" data-href="/results/220918_222206_FP">
            # <td>
            #     Sun, 18 Sep 2022 22:22:06 UTC
            # </td>
            # <td>
            #     Practice
            # </td>
            # <td>
            #     Suzuka
            # </td>
            # <td>
            #     <small>Kevin Unda</small>
            # </td>
            #  <td class="text-center">
            # <a class="text-primary hover-button pl-1 pr-1" href="/results/download/220918_222206_FP.json"></a>
            # </td>
            ###
            response = requests.get(url)
            if response.status_code == 404:
                print(f"Page {page_num} not found on server {server_num}")
                break

            if response.status_code != 200:
                print(f"Failed to retrieve page {page_num} from server {server_num}")
                continue

            soup = bs4(response.content, "html.parser")
            table = soup.find("table")
            if not table:
                print(
                    f"No table found on page {page_num} from server {server_num}",
                )
                continue

            rows = table.find_all("tr", class_="row-link")
            existing_file_found = False
            for row in rows:
                if existing_file_found:
                    break

                row: Tag
                cells: list[Tag] = row.find_all("td")
                if len(cells) != 5:
                    continue

                # date = datetime.strptime(
                #     cells[0].text.strip(), "%a, %d %b %Y %H:%M:%S %Z"
                # )
                session_type = cells[1].text.strip()
                if False:
                    continue

                # elif end_date >= date >= start_date:
                #     pass

                # is team series
                elif server_num in team_series_servers:
                    # is not race or quali
                    if session_type not in {"Race", "Qualifying"}:
                        continue

                # is endurance series
                elif server_num in endurance_series_servers:
                    # is not race
                    if session_type not in {"Race"}:
                        continue
                # is mcm or liaw series
                elif server_num in mcm_liaw_servers:
                    # is not race or quali
                    if session_type not in {"Race", "Qualifying"}:
                        continue

                download_link = row.find("a", href=True)
                if not download_link:
                    continue

                track_name = cells[2].text.strip().replace(" ", "_")

                file_url = f"https://accsm{server_num}.simracingalliance.com{download_link['href']}"
                file_name = download_link["href"].split("/")[-1].split(".json")[0]
                download_dir = {
                    "Race": "races",
                    "Qualifying": "qualifyings",
                    "Practice": "practices",
                }
                current_dir = os.path.dirname(__file__)
                downloads_dir = os.path.join(current_dir, "downloads")
                file_path = os.path.join(
                    downloads_dir,
                    download_dir[session_type],
                    f"{track_name}_{file_name}_server{server_num}.json",
                )  # "accsm/downloads/races|qualifyings|practices/track_name_file_name.json"

                if os.path.exists(file_path):
                    existing_file_found = True
                    continue

                wait = 1
                while True:
                    status_code = download_file(file_url, file_path)
                    if status_code == 200:
                        print(f"Downloaded {file_url}")
                        break
                    elif status_code == 429:
                        print(f"Rate limited. Waiting {wait} seconds...")
                        time.sleep(wait)
                        wait *= 2
                    else:
                        raise Exception(f"Failed to download {file_url}")

            if existing_file_found:
                break

            page_num += 1
