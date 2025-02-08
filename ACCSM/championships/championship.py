from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional

import requests
from bs4 import BeautifulSoup as bs4
from bs4.element import Tag


@dataclass
class Weather:
    """
    <li>Ambient Temperature: 20°C</li>
    <li>Track Temperature: 24°C</li>
    <li>Cloud Level: 0.2</li>
    <li>Rain: 0</li>
    <li>Weather Randomness: 1</li>
    <li>Sim Racer Weather Conditions: 1</li>
    """

    ambient_temperature: int
    track_temperature: int
    cloud_level: float
    rain: int
    weather_randomness: int
    sim_racer_weather_conditions: int

    @staticmethod
    def from_event_div_str(event_div_str: str) -> Weather:
        get_value = lambda d1, d2: event_div_str.split(d1)[1].split(d2)[0]
        return Weather(
            ambient_temperature=float(get_value("Ambient Temperature: ", "°C")),
            track_temperature=float(get_value("Track Temperature: ", "°C")),
            cloud_level=float(get_value("Cloud Level: ", "</li>")),
            rain=float(get_value("Rain: ", "</li>")),
            weather_randomness=float(get_value("Weather Randomness: ", "</li>")),
            sim_racer_weather_conditions=float(
                get_value("Sim Racer Weather Conditions: ", "</li>")
            ),
        )

    @staticmethod
    def from_json(weather: dict) -> Weather:
        return Weather(
            ambient_temperature=weather["ambient_temperature"],
            track_temperature=weather["track_temperature"],
            cloud_level=weather["cloud_level"],
            rain=weather["rain"],
            weather_randomness=weather["weather_randomness"],
            sim_racer_weather_conditions=weather["sim_racer_weather_conditions"],
        )


@dataclass
class Event:
    event_id: str
    track_name: str
    weather: Weather

    @staticmethod
    def get_track_name_from_event_div(event_div: Tag) -> str:
        event_description_div = event_div.find("div", class_="event-description")
        row = event_description_div.find("div", class_="row")
        track_name = row.find("li").text
        return track_name

    @staticmethod
    def from_event_div(event_div: Tag) -> Event:
        return Event(
            # attrs: {'id' = 'event-details-05a33ce2-50ac-4095-b14b-f437b2aec2a5'}
            event_id=event_div.attrs["id"].split("event-details-")[-1],
            track_name=Event.get_track_name_from_event_div(event_div),
            weather=Weather.from_event_div_str(str(event_div)),
        )

    @staticmethod
    def from_json(event: dict) -> Event:
        return Event(
            event_id=event["event_id"],
            track_name=event["track_name"],
            weather=Weather.from_json(event["weather"]),
        )


@dataclass
class EventResult:
    event_id: str
    points: int

    @staticmethod
    def from_json(event_result: dict) -> EventResult:
        return EventResult(
            event_id=event_result["event_id"],
            points=event_result["points"],
        )


@dataclass
class ChampionshipTeam:
    name: str
    event_results: dict[str, EventResult]

    @staticmethod
    def from_json(team: dict) -> ChampionshipTeam:
        return ChampionshipTeam(
            name=team["name"],
            event_results={
                event_id: EventResult.from_json(team["event_results"][event_id])
                for event_id in team["event_results"]
            },
        )


@dataclass
class ChampionshipDriver:
    name: str
    driver_id: str
    car_model: str
    teams: dict[str, ChampionshipTeam]
    points: int
    penalty_points: int
    position: int
    ignored_event_ids: list[str] = field(default_factory=list)

    @staticmethod
    def from_json(driver: dict) -> ChampionshipDriver:
        return ChampionshipDriver(
            name=driver["name"],
            driver_id=driver["driver_id"],
            car_model=driver["car_model"],
            teams={
                team_name: ChampionshipTeam.from_json(driver["teams"][team_name])
                for team_name in driver["teams"]
            },
            points=driver["points"],
            penalty_points=driver["penalty_points"],
            position=driver["position"],
            ignored_event_ids=driver["ignored_event_ids"],
        )


@dataclass
class DriverStandings:
    drivers: list[ChampionshipDriver]
    championship_id: str
    season: int
    server: int

    @staticmethod
    def from_json(driver_standings: dict) -> DriverStandings:
        return DriverStandings(
            drivers=[
                ChampionshipDriver.from_json(d) for d in driver_standings["drivers"]
            ],
            championship_id=driver_standings["championship_id"],
            season=driver_standings["season"],
            server=driver_standings["server"],
        )


@dataclass
class TeamStandings:
    teams: list[ChampionshipTeam]
    championship_id: str
    season: int
    server: int

    @staticmethod
    def from_json(team_standings: dict) -> TeamStandings:
        return TeamStandings(
            teams=[ChampionshipTeam.from_json(t) for t in team_standings["teams"]],
            championship_id=team_standings["championship_id"],
            season=team_standings["season"],
            server=team_standings["server"],
        )


@dataclass
class PointsReference:
    points: dict[str, int]

    def from_div(div: Tag) -> PointsReference:
        """
        <div class="table-responsive">
                <table class="table table-bordered table-striped">
                    <tbody><tr>
                        <th>Place</th>
                        <th>Points</th>
                    </tr>



                            <tr>
                                <td>1st</td>
                                <td>110</td>
                            </tr>
        """
        points = {}
        rows = div.find_all("tr")
        for row in rows:
            row: Tag
            place: Tag
            points_: Tag
            cols = row.find_all("td")
            if len(cols) != 2:
                continue
            place, points_ = cols
            if place.text in ["Fastest Race Lap", "Best Qualifying Lap"]:
                points[place.text] = int(points_.text)
            else:
                points[PointsReference.pos_str_to_int(place.text)] = int(points_.text)
        return PointsReference(points)

    def from_json(points_reference: dict) -> PointsReference:
        return PointsReference(points=points_reference["points"])

    @staticmethod
    def pos_str_to_int(pos_str: str) -> int:
        return int(
            pos_str.replace("st", "")
            .replace("nd", "")
            .replace("rd", "")
            .replace("th", "")
        )


@dataclass
class Championship:
    server: int
    championship_id: str
    season: int
    name: str
    events: dict[str, Event]
    driver_standings: DriverStandings = None
    team_standings: TeamStandings = None
    points_reference: PointsReference = None

    def __hash__(self):
        return hash(self.championship_id)

    def __str__(self):
        return f"{self.name} Season {self.season} on server {self.server}"

    @property
    def url(self):
        return f"/championship/{self.championship_id}"

    @staticmethod
    def from_cell(cell: Tag, server: int, championship_name: str) -> Championship:
        season = int(championship_name.split("Season")[1].split("|")[0].strip())
        championship_id = cell.find("a")["href"].split("/")[-1]
        return Championship(
            server=server,
            championship_id=championship_id,
            season=season,
            name=championship_name,
            events={},
        )

    @staticmethod
    def from_json(championship: dict) -> Optional[Championship]:
        if not championship["driver_standings"]:
            return None
        if not championship["team_standings"]:
            return None

        return Championship(
            server=championship["server"],
            championship_id=championship["championship_id"],
            season=championship["season"],
            name=championship["name"],
            events={
                e: Event.from_json(championship["events"][e])
                for e in championship["events"]
            },
            driver_standings=DriverStandings.from_json(
                championship["driver_standings"]
            ),
            team_standings=TeamStandings.from_json(championship["team_standings"]),
            points_reference=PointsReference.from_json(
                championship["points_reference"]
            ),
        )

    def set_event_details(self) -> Championship:
        print(f"Getting event details for {self}...")
        response = requests.get(
            f"https://accsm{self.server}.simracingalliance.com{self.url}"
        )
        soup = bs4(response.content, "html.parser")

        events_div = soup.find("div", class_="championship-events")
        if not events_div:
            return

        buttons = events_div.find_all("button", {"data-target": True})
        for button in buttons:
            data_target = button["data-target"]
            # '#event-details-05a33ce2-50ac-4095-b14b-f437b2aec2a5'
            event_div = soup.find("div", id=data_target.strip("#"))
            if event_div:
                event = Event.from_event_div(event_div)
                self.events[event.event_id] = event

    def set_driver_standings(self, driver_standings: dict) -> Championship:
        print(f"Getting driver standings for {self}...")
        """
        {
            "DriverName": "Jeff Spangler",
            "DriverGUID": "S76561198986746493",
            "CarModel": "McLaren 720S GT3 Evo 2023",
            "IsPlayer": true,
            "Teams": {
                "Madhaus Racing | The McKarens": {
                    "EventIDs": {
                        "046e01d0-491f-4fcf-b0f9-8edb75e9ba18": {
                            "RaceNumber": 9,
                            "Points": 80
                        },
                        "05a33ce2-50ac-4095-b14b-f437b2aec2a5": {
                            "RaceNumber": 9,
                            "Points": 85
                        },
                        "7561cf57-cb2b-4b58-8ebc-ee9ce091eb43": {
                            "RaceNumber": 9,
                            "Points": 110
                        },
                        "c7f960b4-9b9d-4eeb-8472-1439a86074c0": {
                            "RaceNumber": 9,
                            "Points": 111
                        },
                        "e3984691-8607-448c-bdac-812cb201aa22": {
                            "RaceNumber": 9,
                            "Points": 85
                        }
                    },
                    "Points": 472
                }
            },
            "Points": 392,
            "PointsPenalty": 0,
            "Position": 1,
            "Even": false,
            "IgnoredEventIDs": {
                "046e01d0-491f-4fcf-b0f9-8edb75e9ba18": {}
            }
        },
        """
        drivers: list[ChampionshipDriver] = []
        for driver in driver_standings:
            teams: dict[str, ChampionshipTeam] = {}
            for team_name, team in driver["Teams"].items():
                event_results: dict[str, EventResult] = {}
                for event_id, event in team["EventIDs"].items():
                    event_results[event_id] = EventResult(
                        event_id=event_id, points=event["Points"]
                    )

                teams[team_name] = ChampionshipTeam(
                    name=team_name, event_results=event_results
                )

            drivers.append(
                ChampionshipDriver(
                    name=driver["DriverName"],
                    driver_id=driver["DriverGUID"],
                    car_model=driver["CarModel"],
                    teams=teams,
                    points=driver["Points"],
                    penalty_points=driver["PointsPenalty"],
                    position=driver["Position"],
                    ignored_event_ids=driver["IgnoredEventIDs"],
                )
            )

        self.driver_standings = DriverStandings(
            drivers=drivers,
            championship_id=self.championship_id,
            season=self.season,
            server=self.server,
        )

        return self

    def set_team_standings(self, team_standings: dict) -> Championship:
        print(f"Getting team standings for {self}...")
        """
        {
            "TeamName": "Visceral x FRT NobaRS",
            "Points": 588,
            "PointsPenalty": 0,
            "IgnoredEventIDs": {
                "c7f960b4-9b9d-4eeb-8472-1439a86074c0": {}
            }
        },
        """
        teams: list[ChampionshipTeam] = []
        for team in team_standings:
            teams.append(
                ChampionshipTeam(
                    name=team["TeamName"],
                    event_results={},
                )
            )

        self.team_standings = TeamStandings(
            teams=teams,
            championship_id=self.championship_id,
            season=self.season,
            server=self.server,
        )

        return self

    def set_standings(self) -> Championship:
        print(f"Getting standings for {self}...")
        # https://accsm1.simracingalliance.com/api/championship/2c6d97ef-343b-45bb-810f-3b2b40296028/standings.json
        wait = 1
        while True:
            response = requests.get(
                f"https://accsm{self.server}.simracingalliance.com/api/championship/{self.championship_id}/standings.json"
            )
            if response.status_code == 429:
                print(f"Rate limited. Waiting {wait} seconds...")
                time.sleep(wait)
                wait *= 2
                continue

            standings = response.json()
            driver_standings = standings["DriverStandings"]
            team_standings = standings["TeamStandings"]
            if "" not in driver_standings or "" not in team_standings:
                print(f"Standings not found for {self}")
                return self
            driver_standings = driver_standings[""]
            team_standings = team_standings[""]
            if driver_standings:
                self.set_driver_standings(driver_standings)
            if team_standings:
                self.set_team_standings(standings["TeamStandings"][""])
            return self

    def set_points_reference(self) -> Championship:
        print(f"Getting points reference for {self}...")
        # https://accsm1.simracingalliance.com/championship/2c6d97ef-343b-45bb-810f-3b2b40296028
        wait = 1
        while True:
            response = requests.get(
                f"https://accsm{self.server}.simracingalliance.com/championship/{self.championship_id}"
            )
            if response.status_code == 429:
                print(f"Rate limited. Waiting {wait} seconds...")
                time.sleep(wait)
                wait *= 2
                continue

            soup = bs4(response.content, "html.parser")
            points_div = soup.find("div", id="points")
            if points_div:
                self.points_reference = PointsReference.from_div(points_div)
            return self
