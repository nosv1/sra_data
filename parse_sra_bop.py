from __future__ import annotations

import os
from dataclasses import dataclass, field

import pandas as pd
import requests
from bs4 import BeautifulSoup

from utils.queries import CarModel


@dataclass
class Car:
    image_url: str
    car_model: CarModel
    tracks: dict[str, int] = field(default_factory=dict)


def download_file(url: str, filepath: os.path):
    response = requests.get(url)
    if response.status_code == 200:
        with open(filepath, "wb") as f:
            f.write(response.content)
    return response.status_code


def parse_sra_bop():
    url = "https://www.simracingalliance.com/about/custom_bop"
    # example header
    # <th class="text-center" style="overflow-wrap: break-word;">
    #     <img class="img-fluid" style="width: 26px; margin: 0px 6px 0px 2px;" src="https://static.simracingalliance.com/assets/images/logo/manufacturers/light/aston_martin.png" alt="" title="" id="exifviewer-img-10" exifid="1036460386" oldsrc="https://static.simracingalliance.com/assets/images/logo/manufacturers/light/aston_martin.png">
    #     <br>
    #     <span data-bs-toggle="tooltip" data-bs-placement="bottom" data-bs-original-title="Year:2019" aria-describedby="tooltip168682">AMR V8 Vantage GT3</span>
    # </th>

    # example row
    # <tr>
    # <!-- Track -->
    # <td class="small">Barcelona</td>
    # <!-- Car -->
    #     <td class="small text-center ballast ballast-5kg" style="max-width:4.6%">-5</td>
    #     ...
    # </tr>

    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    cars: dict[str, Car] = {}
    for car_idx, th in enumerate(soup.find_all("th")):
        img = th.find("img")
        if img:
            image_url = img["src"]
            name = th.find("span").text.strip()
            car_model = CarModel.from_model_name(name)
            cars[car_idx] = Car(image_url, car_model)

    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) == 0:
            continue
        track = tds[0].text
        for car_idx, td in enumerate(tds[1:]):
            if td.text:
                td_text: str = td.text.strip()
                bop: int = int(td_text) if td_text else 0
                cars[car_idx + 1].tracks[track] = bop

    return cars


if __name__ == "__main__":
    cars = parse_sra_bop()

    current_dir = os.path.dirname(__file__)

    data = {}
    for car in cars.values():
        download_file(car.image_url, car.car_model.get_logo_path(current_dir))

        for track, bop in car.tracks.items():
            if track not in data:
                data[track] = {}
            data[track][car.car_model.name] = bop

    df = pd.DataFrame(data).T.fillna(0)
    print(df)
