import json
import os
import sys

current_dir = os.path.dirname(__file__)
for folder in ["practices", "qualifyings", "races"]:
    download_dir = os.path.join(current_dir, folder)
    for file in os.listdir(download_dir):
        if file.endswith(".json") and file.startswith("_"):
            results_file = json.load(open(os.path.join(download_dir, file), "r"))
            session_result = results_file["sessionResult"]
            leaderboard_lines = session_result["leaderBoardLines"]
            for line in leaderboard_lines:
                car = line["car"]
                drivers = car["drivers"]
                drivers_dict = {}
                for driver in drivers:
                    player_id = driver["playerId"]
                    member_id = driver["memberId"]
                    if player_id == "PlayerID":
                        driver["playerId"] = member_id
                    drivers_dict[member_id] = driver
                    pass
                current_driver = line["currentDriver"]
                current_driver["playerId"] = drivers_dict[current_driver["memberId"]][
                    "playerId"
                ]
                pass
            json.dump(
                results_file, open(os.path.join(download_dir, file), "w"), indent=4
            )
            pass
        pass
    pass
pass
