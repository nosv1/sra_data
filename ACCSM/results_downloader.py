import json
import os
import time
from datetime import datetime, timezone

import requests


def try_get_json(url: str, wait: int = 5):
    total_waited = 0
    while True:
        response = requests.get(url)
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

def get_accsm_results():
    # https://wiki.emperorservers.com/assetto-corsa-competizione-server-manager/web-api
    # API endpoints are rate limited to a maximum of 5 requests per 20 seconds. You will receive a "Too Many Requests" error if you exceed the limit. We recommend that you do not request data from the API more than twice per minute.

    base_url = lambda server: f"https://accsm{server}.simracingalliance.com"
    results_list_url = (
        lambda server, page: f"{base_url(server)}/api/results/list.json?page={page}&sort=date"
    )
    results_file_url = (
        lambda server, filename: f"{base_url(server)}/results/download/{filename}.json"
    )
    folder_from_session_type = lambda session_type: {
        "FP": "practices",
        "Q": "qualifyings",
        "Q1": "qualifyings",
        "Q2": "qualifyings",
        "R": "races",
        "R1": "races",
        "R2": "races",
        "R3": "races",
    }.get(session_type, "unknown")
    construct_filename = (
        lambda session_type, track, accsm_file, server: f"{folder_from_session_type(session_type)}/{track}_{accsm_file}_{server}.json"
    )

    current_dir = os.path.dirname(__file__)
    downloads_dir = os.path.join(current_dir, "downloads")

    # listed out to allow for easy commenting out of servers
    servers = [1, 2, 3, 4, 5, 6, 7]

    for server in servers:
        page = 0
        num_pages = float("inf")
        file_exists = False
        while page < num_pages and not file_exists:
            # "num_pages": 397,
            # "current_page": 0,
            # "sort_type": "date",
            # "results": [ ... ]
            results_list_json = try_get_json(results_list_url(server, page))
            num_pages = results_list_json["num_pages"]
            print(f"Server {server} - Page {page}/{num_pages}")

            if not num_pages:
                break

            for result in results_list_json["results"]:
                # 'track' = 'valencia'
                # 'session_type' = 'R'
                # 'date' = '2025-02-19T22:40:20Z'
                # 'results_json_url' = '/results/download/250219_224020_R.json'
                # 'results_page_url' = '/results/250219_224020_R'
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

                filename = construct_filename(session_type, track, accsm_file, server)
                path = os.path.join(downloads_dir, filename)
                if os.path.exists(path):
                    print(f"File {path} already exists... skipping {filename}")
                    # continue
                    file_exists = True
                    break

                # download the file
                file_url = results_file_url(server, accsm_file)
                results_file_json = try_get_json(file_url)
                with open(path, "w") as f:
                    json.dump(results_file_json, f, indent=4)
                print(f"Downloaded {filename}")
                pass

            pass

            page += 1
        pass


if __name__ == "__main__":
    get_accsm_results()
    pass
