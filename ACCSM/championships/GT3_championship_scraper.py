import json
import os

from championship import *


def get_championships() -> list[Championship]:
    championships: list[Championship] = []
    for server in range(1, 9):
        print(f"Getting championship urls for server {server}...")
        url = f"https://accsm{server}.simracingalliance.com/championships"
        response = requests.get(url)
        soup = bs4(response.content, "html.parser")

        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            for row in rows:
                cell = row.find("td")
                if not cell:
                    continue

                # TT | Division 2 | Season 12 | GT3 Team Championship
                championship_name = cell.find("a").text
                if (
                    cell
                    and cell.find("a")
                    and "GT3 Team Championship" in championship_name
                ):
                    championship = Championship.from_cell(
                        cell, server, championship_name
                    )
                    championship.set_event_details()
                    championship.set_standings()
                    championship.set_points_reference()
                    championships.append(championship)

    return championships


if __name__ == "__main__":
    current_dir = os.path.dirname(__file__)

    championships: list[Championship] = get_championships()
    championships.sort(key=lambda c: (-c.season, c.server))
    json.dump(
        championships,
        open(os.path.join(current_dir, "championships.json"), "w"),
        indent=4,
        default=lambda o: o.__dict__ if hasattr(o, "__dict__") else str(o),
    )
    pass
