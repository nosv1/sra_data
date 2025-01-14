from __future__ import annotations

import os
import sys
import timeit
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum as enum
from statistics import mean
from typing import Optional

from neo4j import Driver as Neo4jDriver
from neo4j._work.eager_result import EagerResult
from pytz import timezone as pytz_timezone
from scipy.stats import linregress

from Database import Neo4jDatabase


class SessionTypes(enum):
    RACE = "R"
    RACE1 = "R1"
    RACE2 = "R2"
    QUALIFYING = "Q"
    FREE_PRACTICE = "FP"


class CarModels:
    model_id_dict: dict[int, str] = dict(
        {
            0: "Porsche 991 GT3 R",
            1: "Mercedes-AMG GT3",
            2: "Ferrari 488 GT3",
            3: "Audi R8 LMS",
            4: "Lamborghini Huracan GT3",
            5: "McLaren 650S GT3",
            6: "Nissan GT-R Nismo GT3 2018",
            7: "BMW M6 GT3",
            8: "Bentley Continental GT3 2018",
            9: "Porsche 991II GT3 Cup",
            10: "Nissan GT-R Nismo GT3 2017",
            11: "Bentley Continental GT3 2016",
            12: "Aston Martin V12 Vantage GT3",
            13: "Lamborghini Gallardo R-EX",
            14: "Jaguar G3",
            15: "Lexus RC F GT3",
            16: "Lamborghini Huracan Evo (2019)",
            17: "Honda NSX GT3",
            18: "Lamborghini Huracan SuperTrofeo",
            19: "Audi R8 LMS Evo (2019)",
            20: "AMR V8 Vantage (2019)",
            21: "Honda NSX Evo (2019)",
            22: "McLaren 720S GT3 (2019)",
            23: "Porsche 911II GT3 R (2019)",
            24: "Ferrari 488 GT3 Evo 2020",
            25: "Mercedes-AMG GT3 2020",
            26: "Ferrari 488 Challenge Evo",
            27: "BMW M2 CS Racing",
            28: "Porsche 911 GT3 Cup (Type 992)",
            29: "Lamborghini Huracán Super Trofeo EVO2",
            30: "BMW M4 GT3",
            31: "Audi R8 LMS GT3 evo II",
            32: "Ferrari 296 GT3",
            33: "Lamborghini Huracan Evo2",
            34: "Porsche 992 GT3 R",
            35: "McLaren 720S GT3 Evo 2023",
            36: "Ford Mustang GT3",
            50: "Alpine A110 GT4",
            51: "AMR V8 Vantage GT4",
            52: "Audi R8 LMS GT4",
            53: "BMW M4 GT4",
            55: "Chevrolet Camaro GT4",
            56: "Ginetta G55 GT4",
            57: "KTM X-Bow GT4",
            58: "Maserati MC GT4",
            59: "McLaren 570S GT4",
            60: "Mercedes-AMG GT4",
            61: "Porsche 718 Cayman GT4",
            80: "Audi R8 LMS GT2",
            82: "KTM XBOW GT2",
            83: "Maserati MC20 GT2",
            84: "Mercedes AMG GT2",
            85: "Porsche 911 GT2 RS CS Evo",
            86: "Porsche 935",
        }  # acc model id -> acc name
    )
    sra_corrections: dict[str, str] = dict(
        {
            "AMR V8 Vantage GT3": "AMR V8 Vantage (2019)",
            "Audi R8 LMS GT3 Evo 2": "Audi R8 LMS Evo (2019)",
            "Bentley Continental GT3": "Bentley Continental GT3 2018",
            "Emil Frey Jaguar G3": "Jaguar G3",
            "Ferrari 488 GT3 Evo": "Ferrari 488 GT3 Evo 2020",
            "Honda NSX GT3 Evo": "Honda NSX Evo (2019)",
            "Lamborghini Huracán GT3 EVO2": "Lamborghini Huracan Evo2",
            "McLaren 720S GT3 Evo": "McLaren 720S GT3 Evo 2023",
            "Mercedes-AMG GT3 EVO": "Mercedes-AMG GT3 2020",
            "Nissan GT-R Nismo GT3": "Nissan GT-R Nismo GT3 2017",
            "Porsche 991 II GT3 R": "Porsche 911II GT3 R (2019)",
            "Reiter Engineering R-EX GT3": "Lamborghini Gallardo R-EX",
        }  # sra name -> acc name
    )
    model_name_dict: dict[str, int] = {v: k for k, v in model_id_dict.items()}


class CarModel:
    def __init__(self, model_id: int, name: str):
        self.model_id = model_id
        self.name = name

    def from_model_id(model_id: int) -> CarModel:
        return CarModel(model_id=model_id, name=CarModels.model_id_dict[model_id])

    def from_model_name(name: str) -> CarModel:
        name = CarModels.sra_corrections.get(name, name)
        return CarModel(model_id=CarModels.model_name_dict[name], name=name)

    @property
    def logo_file_name(self) -> str:
        return f"{'_'.join(self.name.split(' '))}.png"

    def get_logo_path(self, current_dir: str) -> str:
        manufacturers_dir = os.path.join(
            current_dir, "assets", "images", "logo", "manufacturers"
        )
        return os.path.join(
            manufacturers_dir,
            self.logo_file_name,
        )


class TrackNames(enum):
    KYALAMI = "kyalami"
    MISANO = "misano"
    NURBURGRING = "nurburgring"
    SUZUKA = "suzuka"
    WATKINS_GLEN = "watkins_glen"
    ZANDVOORT = "zandvoort"


@dataclass
class SRADriver:
    driver_id: str
    member_id: str
    first_name: str
    last_name: str
    division: float
    sessions: Optional[list[Session]] = None
    cars: Optional[list[Car]] = None

    def from_node(driver_node) -> SRADriver:
        return SRADriver(
            driver_id=driver_node["driver_id"],
            member_id=driver_node["member_id"],
            first_name=driver_node["first_name"],
            last_name=driver_node["last_name"],
            division=driver_node["division"],
        )

    def from_record(record, driver_node_key="d") -> SRADriver:
        return SRADriver.from_node(record[driver_node_key])

    def set_cars(self, cars: list[Car]) -> SRADriver:
        self.cars = cars
        return self

    def set_sessions(self, sessions: list[Session]) -> SRADriver:
        """Sessions should always be ordered by session_file oldest first"""
        self.sessions = sessions
        return self

    @property
    def name(self) -> str:
        return f"{self.first_name} {self.last_name}"

    @property
    def race_division(self) -> int:
        return int(self.division) if self.division else None

    def cal_avg_ts_apd(
        self,
        max_sessions: int = 8,
        min_sessions: int = 3,
        after_date: datetime = (datetime.now() - timedelta(days=365)).replace(
            tzinfo=pytz_timezone("UTC")
        ),
    ) -> Optional[float]:
        """Returns the average of the last max_sessions ts_avg_percent_diff values"""
        apds: list[float] = []
        for c, s in zip(self.cars, self.sessions):
            c: Car
            s: Session
            if c.ts_avg_percent_diff and s.finish_time > after_date:
                apds.append(c.ts_avg_percent_diff)

        return (
            mean(apds[-min(max_sessions, len(apds)) :])
            if len(apds) >= min_sessions
            else None
        )


@dataclass
class Lap:
    lap_time: float
    lap_number: int
    running_session_lap_count: int
    split1: float
    split2: float
    split3: float
    is_valid_for_best: bool
    session_file: str
    driver_id: Optional[str] = None

    def from_lap_node(lap_node) -> Lap:
        return Lap(
            lap_time=lap_node["lap_time"],
            lap_number=lap_node["lap_number"],
            running_session_lap_count=lap_node["running_session_lap_count"],
            split1=lap_node["split1"],
            split2=lap_node["split2"],
            split3=lap_node["split3"],
            is_valid_for_best=lap_node["is_valid_for_best"],
            session_file=lap_node["session_file"],
            driver_id=lap_node["driver_id"],
        )

    def from_record(record, lap_time_node_key="l"):
        return Lap.from_lap_node(record[lap_time_node_key])

    @property
    def splits(self) -> list[int]:
        return [self.split1, self.split2, self.split3]


@dataclass
class Stint:
    laps: list[Lap]

    @property
    def trend(self) -> float:
        linregress_result = linregress(
            [lap.lap_number for lap in self.laps],
            [lap.lap_time for lap in self.laps],
        )
        return linregress_result.slope


@dataclass
class SessionDriver:
    driver_id: str
    first_name: str
    last_name: str
    division: float | None
    car: Optional[Car] = None
    laps: Optional[list[Lap]] = None

    # best possibly invalid times
    best_split1: Optional[Lap] = None
    best_split2: Optional[Lap] = None
    best_split3: Optional[Lap] = None
    best_lap: Optional[Lap] = None

    # best guaranteed valid times
    best_valid_split1: Optional[Lap] = None
    best_valid_split2: Optional[Lap] = None
    best_valid_split3: Optional[Lap] = None
    best_valid_lap: Optional[Lap] = None

    # race specific fields
    start_offset: Optional[int] = None
    start_position: Optional[int] = None
    lap_running_time: Optional[list[int]] = None
    split_running_time: Optional[list[int]] = None
    gap_to_leader_per_split: Optional[list[int]] = None
    probable_pit_laps: Optional[list[int]] = None
    stints: Optional[list[Stint]] = None

    def __eq__(self, other: SessionDriver):
        return self.driver_id == other.driver_id

    def __hash__(self):
        return hash(self.driver_id)

    def from_node(driver_node) -> SessionDriver:
        return SessionDriver(
            driver_id=driver_node["driver_id"],
            first_name=driver_node["first_name"],
            last_name=driver_node["last_name"],
            division=driver_node["division"],
        )

    def from_record(record, driver_node_key="d") -> SessionDriver:
        return SessionDriver.from_node(record[driver_node_key])

    def set_car(self, car: Car):
        self.car = car

    def set_laps(self, laps: list[Lap]):
        self.laps = laps
        if not self.laps:
            return

        # this is the very first time the driver finishes split 1
        # offset is the difference between finish line to lap 1 split 1 and from the time the car spawned in to lap 1 split 1
        # the offset will be added to all the running times to get the correct time
        lap1_split1 = self.car.total_time - (self.sum_splits - self.laps[0].split1)
        offset = lap1_split1 - self.laps[0].split1
        self.set_start_offset(offset)

        self.lap_running_time = [self.start_offset]
        self.split_running_time = [self.start_offset]
        self.probable_pit_laps = []
        self.stints = []
        min_split_3_1_combo = float("inf")

        self.best_split1 = self.laps[0]
        self.best_split2 = self.laps[0]
        self.best_split3 = self.laps[0]
        self.best_lap = self.laps[0]

        for l_idx, lap in enumerate(laps):
            self.lap_running_time.append(self.lap_running_time[-1] + lap.lap_time)

            for split in lap.splits:
                self.split_running_time.append(self.split_running_time[-1] + split)

            # after first lap, we can start looking for probable pit laps
            if l_idx:
                min_split_3_1_combo = min(
                    min_split_3_1_combo,
                    lap.split1 + laps[l_idx - 1].split3,
                )

            # try update best lap and splits (and valid versions)
            if lap.is_valid_for_best:
                if (
                    not self.best_valid_split1
                    or lap.split1 < self.best_valid_split1.split1
                ):
                    self.best_valid_split1 = lap
                if (
                    not self.best_valid_split2
                    or lap.split2 < self.best_valid_split2.split2
                ):
                    self.best_valid_split2 = lap
                if (
                    not self.best_valid_split3
                    or lap.split3 < self.best_valid_split3.split3
                ):
                    self.best_valid_split3 = lap
                if (
                    not self.best_valid_lap
                    or lap.lap_time < self.best_valid_lap.lap_time
                ):
                    self.best_valid_lap = lap
            # always update best lap and splits
            if lap.split1 < self.best_split1.split1:
                self.best_split1 = lap
            if lap.split2 < self.best_split2.split2:
                self.best_split2 = lap
            if lap.split3 < self.best_split3.split3:
                self.best_split3 = lap
            if lap.lap_time < self.best_lap.lap_time:
                self.best_lap = lap

        # find probable pit laps
        stint = Stint(laps=[])
        for l_idx, lap in enumerate(laps):
            if not l_idx:
                continue
            split_3_1_combo = lap.split1 + laps[l_idx - 1].split3
            if split_3_1_combo - min_split_3_1_combo > 30000:
                self.probable_pit_laps.append(lap.lap_number)
                if stint.laps:
                    stint.laps.pop()
                self.stints.append(stint)
                stint = Stint(laps=[])
            else:
                stint.laps.append(lap)
        self.stints.append(stint)

    def set_start_offset(self, starting_gap: int):
        self.start_offset = starting_gap

    def set_start_position(self, start_position: int):
        self.start_position = start_position

    @property
    def name(self):
        return f"{self.first_name} {self.last_name}"

    @property
    def race_division(self) -> int:
        return int(self.division) if self.division else 0

    @property
    def potential_best_lap_time(self) -> Optional[int]:
        if self.best_lap:
            return sum(
                [
                    self.best_split1.split1,
                    self.best_split2.split2,
                    self.best_split3.split3,
                ]
            )
        return None

    @property
    def potential_best_valid_lap_time(self) -> Optional[int]:
        if self.best_valid_lap:
            return sum(
                [
                    self.best_valid_split1.split1,
                    self.best_valid_split2.split2,
                    self.best_valid_split3.split3,
                ]
            )
        return None

    @property
    def sum_laps(self) -> list[int]:
        if not self.laps:
            return 0
        return sum(l.lap_time for l in self.laps)

    @property
    def sum_splits(self) -> list[int]:
        if not self.laps:
            return 0
        return sum(sum(l.splits) for l in self.laps)

    @property
    def is_silver_driver(self) -> bool:
        return self.division and self.division != float(self.race_division)


@dataclass
class CareerDriver:
    driver: SessionDriver

    # best possibly invalid times
    best_split1s: list[Lap] = field(default_factory=list)
    best_split2s: list[Lap] = field(default_factory=list)
    best_split3s: list[Lap] = field(default_factory=list)
    best_laps: list[Lap] = field(default_factory=list)

    # best guaranteed valid times
    best_valid_split1s: list[Lap] = field(default_factory=list)
    best_valid_split2s: list[Lap] = field(default_factory=list)
    best_valid_split3s: list[Lap] = field(default_factory=list)
    best_valid_laps: list[Lap] = field(default_factory=list)

    # optionals
    _avg_best_lap_time: Optional[int] = None
    _avg_best_valid_lap_time: Optional[int] = None

    def from_drivers(drivers: list[SessionDriver]) -> CareerDriver:
        career_driver = CareerDriver(driver=drivers[0])
        for d_idx, driver in enumerate(drivers):
            if driver.best_lap:
                career_driver.best_split1s.append(driver.best_split1)
                career_driver.best_split2s.append(driver.best_split2)
                career_driver.best_split3s.append(driver.best_split3)
                career_driver.best_laps.append(driver.best_lap)

            if driver.best_valid_lap:
                career_driver.best_valid_split1s.append(driver.best_valid_split1)
                career_driver.best_valid_split2s.append(driver.best_valid_split2)
                career_driver.best_valid_split3s.append(driver.best_valid_split3)
                career_driver.best_valid_laps.append(driver.best_valid_lap)

        if career_driver.best_laps:
            career_driver.best_split1s.sort(key=lambda l: l.split1)
            career_driver.best_split2s.sort(key=lambda l: l.split2)
            career_driver.best_split3s.sort(key=lambda l: l.split3)
            career_driver.best_laps.sort(key=lambda l: l.lap_time)

        if career_driver.best_valid_laps:
            career_driver.best_valid_split1s.sort(key=lambda l: l.split1)
            career_driver.best_valid_split2s.sort(key=lambda l: l.split2)
            career_driver.best_valid_split3s.sort(key=lambda l: l.split3)
            career_driver.best_valid_laps.sort(key=lambda l: l.lap_time)

        return career_driver

    @property
    def potential_best_lap_time(self) -> Optional[int]:
        if self.best_laps:
            return sum(
                [
                    self.best_split1s[0].split1,
                    self.best_split2s[0].split2,
                    self.best_split3s[0].split3,
                ]
            )

    @property
    def potential_best_valid_lap_time(self) -> Optional[int]:
        if self.best_valid_laps:
            return sum(
                [
                    self.best_valid_split1s[0].split1,
                    self.best_valid_split2s[0].split2,
                    self.best_valid_split3s[0].split3,
                ]
            )

    def avg_best_lap_time(self, count: Optional[int] = None) -> Optional[int]:
        if not self._avg_best_lap_time:
            self._avg_best_lap_time = (
                mean([l.lap_time for l in self.best_laps][:count])
                if self.best_laps
                else None
            )
        return self._avg_best_lap_time

    def avg_best_valid_lap_time(self, count: Optional[int] = None) -> Optional[int]:
        if not self._avg_best_valid_lap_time:
            self._avg_best_valid_lap_time = (
                mean([l.lap_time for l in self.best_valid_laps][:count])
                if self.best_valid_laps
                else None
            )
        return self._avg_best_valid_lap_time


@dataclass
class Car:
    car_id: int
    car_model: int
    car_number: int
    finish_position: int
    total_time: int  # this is probably time in control of car, excluding when RTG or car is locked from control (still includes pit time)
    time_in_pits: int
    lap_count: int
    best_split1: int
    best_split2: int
    best_split3: int
    best_lap: int
    avg_percent_diff: Optional[float] = None
    ts_avg_percent_diff: Optional[float] = None

    def try_set_optional_fields(self, car_node: dict) -> Car:
        self.avg_percent_diff = car_node.get("avg_percent_diff")
        self.ts_avg_percent_diff = car_node.get("ts_avg_percent_diff")
        return self

    def from_car_node(car_node) -> Car:
        return Car(
            car_id=car_node["car_id"],
            car_model=car_node["car_model"],
            car_number=car_node["car_number"],
            finish_position=car_node["finish_position"],
            total_time=car_node["total_time"],
            time_in_pits=car_node["time_in_pits"],
            lap_count=car_node["lap_count"],
            best_split1=car_node["best_split1"],
            best_split2=car_node["best_split2"],
            best_split3=car_node["best_split3"],
            best_lap=car_node["best_lap"],
        ).try_set_optional_fields(car_node)

    def from_record(record, car_node_key="c") -> Car:
        return Car.from_car_node(record[car_node_key])

    @property
    def model(self) -> CarModel:
        return CarModel.from_model_id(self.car_model)


@dataclass
class Session:
    key_: str
    track_name: str
    session_type: str
    finish_time: datetime
    session_file: str
    server_number: int
    server_name: str
    drivers: Optional[list[SessionDriver]] = None

    def from_session_node(session_node) -> Session:
        return Session(
            key_=session_node["key_"],
            track_name=session_node["track_name"],
            session_type=session_node["session_type"],
            finish_time=session_node["finish_time"],
            session_file=session_node["session_file"],
            server_number=session_node["server_number"],
            server_name=session_node["server_name"],
        )

    def from_record(record, session_node_key="s"):
        return Session.from_session_node(record[session_node_key])

    def from_complete_record(
        session_record,
        session_node_key="s",
        driver_node_key="d",
        car_node_key="c",
        laps_key="laps",
    ):
        driver = SessionDriver.from_node(session_record[driver_node_key])
        driver.set_car(Car.from_car_node(session_record[car_node_key]))
        driver.set_laps(
            sorted(
                [Lap.from_lap_node(node) for node in session_record[laps_key]],
                key=lambda lap: lap.lap_number,
            )
        )
        return Session(
            key_=session_record[session_node_key]["key_"],
            track_name=session_record[session_node_key]["track_name"],
            session_type=session_record[session_node_key]["session_type"],
            finish_time=session_record[session_node_key]["finish_time"],
            session_file=session_record[session_node_key]["session_file"],
            server_number=session_record[session_node_key]["server_number"],
            server_name=session_record[session_node_key]["server_name"],
            drivers=[driver],
        )

    def set_session_drivers(self, neo_driver: Neo4jDriver) -> Session:
        """Sets the car and laps for each driver in the session and assigns them to the session"""

        drivers: list[SessionDriver] = get_basic_drivers_from_session(neo_driver, self)
        for driver in drivers:
            neo_results = neo_driver.execute_query(
                f"""
                MATCH (s:Session)<-[]-(l:Lap)-[]->(d:Driver)-[]->(c:Car)
                WHERE TRUE
                    AND d.driver_id = '{driver.driver_id}' AND s.key_ = '{self.key_}'
                    AND c.session_file = l.session_file
                RETURN s, d, c, l
                """
            )

            if neo_results.records:
                driver.set_car(Car.from_record(neo_results.records[0]))
                driver.set_laps(
                    sorted(
                        [Lap.from_record(record) for record in neo_results.records],
                        key=lambda lap: lap.lap_number,
                    )
                )
        drivers.sort(
            key=lambda driver: (
                driver.car.finish_position if driver.car else float("inf")
            )
        )
        self.drivers = drivers
        return self

    def set_driver_gaps_to_leader(self) -> Session:
        """
        Sets the gap to leader per lap and split for each driver
        Should probably be used only for race sessions...
        """

        min_running_time_per_lap_per_split = [(float("inf"), None)]
        for d_idx, driver in enumerate(self.drivers):
            # car or laps don't exist
            if not driver.car or not driver.laps:
                driver.split_running_time = [float("inf")]
                driver.gap_to_leader_per_split = [float("inf")]
                continue

            for s_idx, split_running_time in enumerate(driver.split_running_time):
                # add the next split to the list
                if len(min_running_time_per_lap_per_split) - 1 <= s_idx:
                    min_running_time_per_lap_per_split.append(
                        min_running_time_per_lap_per_split[-1]
                    )

                if (
                    # > 0 is a hacky check to handle incorrect start offsets for when drivers'
                    # total times are less than sum of splits
                    # this could happen if a driver RTGs and their car total time doesn't continue counting (i think)
                    split_running_time > 0
                    and split_running_time
                    < min_running_time_per_lap_per_split[s_idx][0]
                ):
                    min_running_time_per_lap_per_split[s_idx] = (
                        split_running_time,
                        d_idx,
                    )

        for d_idx, driver in enumerate(self.drivers):
            driver.gap_to_leader_per_split = []
            for s_idx, running_time in enumerate(driver.split_running_time):
                min_running_time = min_running_time_per_lap_per_split[s_idx][0]
                driver.gap_to_leader_per_split.append(
                    (running_time - min_running_time) / 1000.0
                )

        return self


@dataclass
class TeamSeriesSession:
    session: Session
    season: int
    division: int

    def from_record(record, ts_node_key="ts"):
        return TeamSeriesSession(
            session=Session.from_record(record),
            season=record[ts_node_key]["season"],
            division=record[ts_node_key]["division"],
        )


@dataclass
class TeamSeriesWeekend:
    qualifying: TeamSeriesSession
    race: TeamSeriesSession

    def from_records(records, ts_node_key="ts", s_node_key="s"):
        qualifying: Optional[TeamSeriesSession] = None
        race: Optional[TeamSeriesSession] = None
        for session_record in records:
            ts_session = TeamSeriesSession.from_record(session_record)
            if ts_session.session.session_type == SessionTypes.QUALIFYING.value:
                qualifying = ts_session
            elif ts_session.session.session_type == SessionTypes.RACE.value:
                race = ts_session
        return TeamSeriesWeekend(qualifying=qualifying, race=race)

    def set_drivers(self, neo_driver: Neo4jDriver) -> list[SessionDriver]:
        """Sets the start position for each driver"""

        quali_drivers = {
            d.driver_id: d
            for d in self.qualifying.session.set_session_drivers(neo_driver).drivers
        }
        race_drivers = (
            self.race.session.set_session_drivers(neo_driver)
            .set_driver_gaps_to_leader()
            .drivers
        )
        for race_driver in race_drivers:
            start_position = len(race_drivers)
            if race_driver.driver_id not in quali_drivers:
                race_driver.set_start_position(start_position)
            else:
                quali_driver = quali_drivers[race_driver.driver_id]
                race_driver.set_start_position(
                    quali_driver.car.finish_position
                    if quali_driver.car
                    else start_position
                )

        return race_drivers


def get_query_results(neo_driver: Neo4jDriver, message: str, query: str) -> EagerResult:
    print(f"{message}", end=" ")
    start_time = timeit.default_timer()
    neo_results: EagerResult = neo_driver.execute_query(query)
    elapsed = timeit.default_timer() - start_time
    print(f"retrieved {len(neo_results.records)} records after {elapsed:.2f} seconds")
    return neo_results


def get_session_keys(neo_driver: Neo4jDriver) -> set[str]:
    neo_results: EagerResult = neo_driver.execute_query(
        """
        MATCH (s:Session)
        RETURN s.key_
        """
    )

    return set([record["s.key_"] for record in neo_results.records])


def get_session_by_key(neo_driver: Neo4jDriver, session_key: str):
    neo_results: EagerResult = neo_driver.execute_query(
        f"""
        MATCH (s:Session)
        WHERE s.key_ = '{session_key}'
        RETURN s
        """
    )
    if len(neo_results.records) > 1:
        raise ValueError(f"Found more than one session with key {session_key}")
    return Session.from_record(neo_results.records[0])


def get_team_series_session_by_attrs(
    neo_driver: Neo4jDriver,
    season: int,
    track_name: str,
    division: int,
    session_type: str,
):
    neo_results: EagerResult = neo_driver.execute_query(
        f"""
        MATCH (ts:TeamSeriesSession)-[]->(s:Session)
        WHERE TRUE
            AND ts.season = {season}
            AND s.track_name = '{track_name}'
            AND ts.division = {division}
            AND s.session_type = '{session_type}'
        RETURN s, ts
        """
    )
    if len(neo_results.records) > 1:
        raise ValueError(
            f"Found more than one session with season {season}, track name {track_name}, division {division}, and session type {session_type}"
        )
    return TeamSeriesSession.from_record(neo_results.records[0])


def get_team_series_sessions_by_attrs(
    neo_driver: Neo4jDriver,
    seasons: set[int] = set(),
    track_names: set[str] = set(),
    divisions: set[int] = set(),
    session_types: set[str] = set(),
) -> list[TeamSeriesSession]:
    query = f"""
        MATCH (ts:TeamSeriesSession)-[]->(s:Session)
        WHERE TRUE
    """
    if seasons:
        query += f"AND ts.season IN {list(seasons)}\n"
    if track_names:
        query += f"AND s.track_name IN {list(track_names)}\n"
    if divisions:
        query += f"AND ts.division IN {list(divisions)}\n"
    if session_types:
        query += f"AND s.session_type IN {list(session_types)}\n"
    query += "RETURN s, ts\n"
    query += "ORDER BY s.session_file ASC"

    neo_results: EagerResult = neo_driver.execute_query(query)
    return [TeamSeriesSession.from_record(record) for record in neo_results.records]


def get_team_series_weekend_by_attrs(
    neo_driver: Neo4jDriver, season: int, track_name: str, division: int
) -> TeamSeriesWeekend:
    neo_results: EagerResult = get_query_results(
        neo_driver=neo_driver,
        message=f"Getting team series weekend for season {season}, track {track_name}, division {division}...",
        query=f"""
            MATCH (tsw:TeamSeriesWeekend)-[]->(ts:TeamSeriesSession)-[]->(s:Session)
            WHERE TRUE
                AND tsw.key_ = '{season}_{division}_{track_name}'
            RETURN s, ts
            ORDER BY s.session_file ASC
        """,
    )
    return TeamSeriesWeekend.from_records(neo_results.records)


def get_basic_driver_by_id(neo_driver: Neo4jDriver, driver_id: str) -> SessionDriver:
    neo_results: EagerResult = neo_driver.execute_query(
        f"""
        MATCH (d:Driver)
        WHERE d.driver_id = '{driver_id}'
        RETURN d
        """
    )
    if len(neo_results.records) > 1:
        raise ValueError(f"Found more than one driver with driver_id {driver_id}")
    return SessionDriver.from_record(neo_results.records[0])


def get_basic_driver_by_first_last_name(
    neo_driver: Neo4jDriver, first_name: str, last_name: str
) -> SessionDriver:
    neo_results: EagerResult = neo_driver.execute_query(
        f"""
        MATCH (d:Driver)
        WHERE TRUE
            AND d.first_name = '{Neo4jDatabase.handle_bad_string(first_name)}' 
            AND d.last_name = '{Neo4jDatabase.handle_bad_string(last_name)}'
        RETURN d
        """
    )
    if len(neo_results.records) > 1:
        raise ValueError(
            f"Found more than one driver with first name {first_name} and last name {last_name}"
        )
    return SessionDriver.from_record(neo_results.records[0])


def get_basic_drivers_from_session(
    neo_driver: Neo4jDriver, session: Session
) -> list[SessionDriver]:
    neo_results: EagerResult = neo_driver.execute_query(
        f"""
        MATCH (d:Driver)-[]->(s:Session)
        WHERE s.key_ = '{session.key_}'
        RETURN d
        """
    )
    return [SessionDriver.from_record(record) for record in neo_results.records]


def get_cars_from_session(neo_driver: Neo4jDriver, session: Session) -> list[str]:
    neo_results: EagerResult = neo_driver.execute_query(
        f"""
        MATCH (c:Car)-[]->(s:Session)
        WHERE s.key_ = '{session.key_}'
        RETURN c
        """
    )
    return [Car.from_record(record) for record in neo_results.records]


def get_driver_laps_for_session(
    neo_driver: Neo4jDriver, driver_id: str, session_key: str
):
    neo_results = neo_driver.execute_query(
        f"""
        MATCH (
            (d:Driver)<-[]-(l:Lap)-[]->(s:Session)
        )
        WHERE d.driver_id = '{driver_id}' AND s.key_ = '{session_key}'
        RETURN l
        """
    )
    return sorted(
        [Lap.from_record(record) for record in neo_results.records],
        key=lambda lap: lap.lap_number,
    )


def get_practice_sessions_for_track(
    neo_driver: Neo4jDriver, track_name: str
) -> list[Session]:
    """Get all practice sessions for a track"""
    neo_results: EagerResult = neo_driver.execute_query(
        f"""
        MATCH (s:Session)
        WHERE TRUE
            AND s.track_name = '{track_name}'
            AND s.session_type = '{SessionTypes.FREE_PRACTICE.value}'
        RETURN s
        ORDER BY s.session_file ASC
        """
    )
    return [Session.from_record(record) for record in neo_results.records]


def get_complete_sessions_by_keys(
    neo_driver: Neo4jDriver, session_keys: set[str]
) -> list[Session]:
    neo_results: EagerResult = get_query_results(
        neo_driver=neo_driver,
        message=f"Getting complete session for key(s) {session_keys}...",
        query=f"""
        MATCH (s:Session)<-[:DRIVER_TO_SESSION]-(d:Driver)
        MATCH (d)<-[:LAP_TO_DRIVER]-(l:Lap)-[:LAP_TO_SESSION]->(s)
        MATCH (d)-[:DRIVER_TO_CAR]->(c:Car)-[:CAR_TO_SESSION]->(s)
        WHERE TRUE
            AND s.key_ IN {list(session_keys)}
        RETURN s, d, COLLECT(l) as laps, c
        """,
    )
    # the results are returned per driver, so each session only has one driver, we need to combine them on the session key
    driver_sessions: list[Session] = [
        Session.from_complete_record(record) for record in neo_results.records
    ]
    sessions: dict[str, Session] = {}
    for driver_session in driver_sessions:
        if driver_session.key_ not in sessions:
            sessions[driver_session.key_] = driver_session
            continue
        sessions[driver_session.key_].drivers += driver_session.drivers
    return list(sessions.values())


def get_complete_sessions_for_track(
    neo_driver: Neo4jDriver, track_name: str, after_date: datetime = datetime.min
) -> list[Session]:
    """Get all practice sessions for a track with drivers, cars, and laps"""
    neo_results: EagerResult = get_query_results(
        neo_driver=neo_driver,
        message=f"Getting complete sessions for track {track_name} after {after_date.isoformat()}...",
        query=f"""
        MATCH (s:Session)<-[:DRIVER_TO_SESSION]-(d:Driver)
        MATCH (d)<-[:LAP_TO_DRIVER]-(l:Lap)-[:LAP_TO_SESSION]->(s)
        MATCH (d)-[:DRIVER_TO_CAR]->(c:Car)-[:CAR_TO_SESSION]->(s)
        WHERE TRUE
            AND s.track_name = '{track_name}'
            AND s.finish_time > datetime('{after_date.isoformat()}')
        RETURN s, d, COLLECT(l) as laps, c
        ORDER BY s.session_file ASC
        """,
    )
    # the results are returned per driver, so each session only has one driver, we need to combine them on the session key
    driver_sessions: list[Session] = [
        Session.from_complete_record(record) for record in neo_results.records
    ]
    sessions: dict[str, Session] = {}
    for driver_session in driver_sessions:
        if driver_session.key_ not in sessions:
            sessions[driver_session.key_] = driver_session
            continue
        sessions[driver_session.key_].drivers += driver_session.drivers
    return list(sessions.values())


def get_sra_drivers(
    neo_driver: Neo4jDriver,
    session_types: set[str] = set(),
    after_date: datetime = datetime.min,
) -> list[SRADriver]:
    session_types: list[str] = list(session_types)
    neo_results: EagerResult = get_query_results(
        neo_driver=neo_driver,
        message="Getting drivers and cars...",
        query=f"""
            MATCH (d:Driver)-[:DRIVER_TO_CAR]->(c:Car)-[:CAR_TO_SESSION]->(s:Session)
            WHERE (SIZE({session_types}) = 0
                OR s.session_type IN {session_types})
                AND s.finish_time > datetime('{after_date.isoformat()}')
            WITH s, c, d
            ORDER BY s.session_file ASC
            WITH d, COLLECT(c) as cars, COLLECT(s) as sessions
            RETURN d, cars, sessions
        """,
    )
    return [
        SRADriver.from_record(record)
        .set_cars([Car.from_car_node(car) for car in record["cars"]])
        .set_sessions(
            [Session.from_session_node(session) for session in record["sessions"]]
        )
        for record in neo_results.records
    ]


if __name__ == "__main__":
    import os
    import pickle

    # sessions = get_complete_sessions_for_track(
    #     neo_driver,
    #     TrackNames.WATKINS_GLEN.value,
    #     datetime(2024, 12, 19).replace(tzinfo=pytz_timezone("US/Eastern")),
    # )

    queries_pickle = "queries.pkl"

    if True or not os.path.exists(queries_pickle):
        neo_driver, neo_session = Neo4jDatabase.connect_database("SRA")
        # ts_weekend = get_team_series_weekend_by_attrs(
        #     neo_driver=neo_driver,
        #     season=13,
        #     # track_name=TrackNames.SUZUKA.value,
        #     track_name=TrackNames.WATKINS_GLEN.value,
        #     division=2,
        # )
        # ts_weekend.set_drivers(neo_driver)

        # sra_drivers = get_sra_drivers(neo_driver)

        ts_sessions = get_team_series_sessions_by_attrs(
            neo_driver=neo_driver,
            seasons={12},
            divisions={1, 2, 3},
            session_types={"Q"},
        )

        Neo4jDatabase.close_connection(neo_driver, neo_session)
        with open(queries_pickle, "wb") as f:
            pickle.dump(ts_sessions, f)

    else:
        with open(queries_pickle, "rb") as f:
            ts_sessions = pickle.load(f)
    pass
