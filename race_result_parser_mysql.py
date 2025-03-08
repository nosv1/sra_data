import json
import os
from dataclasses import dataclass
from datetime import datetime

import numpy as np
from mysql.connector.abstracts import MySQLCursorAbstract

import utils.queries as queries
from Database import MySqlDatabase


class Lap:
    car_id_json = "carId"
    driver_index_json = "driverIndex"
    is_valid_for_best_json = "isValidForBest"
    lap_time_json = "laptime"
    splits_json = "splits"
    split1_json = "split1"
    split2_json = "split2"
    split3_json = "split3"

    def __init__(
        self,
        car_id: int,
        driver_index: int,
        is_valid_for_best: bool,
        lap_time: int,
        splits: list[int],
    ) -> None:
        self.car_id = car_id
        self.driver_index = driver_index
        self.is_valid_for_best = is_valid_for_best
        self.lap_time = lap_time
        self.splits = splits

    @property
    def lap_number(self) -> int:
        return self.__lap_number

    @lap_number.setter
    def lap_number(self, lap_number: int) -> None:
        self.__lap_number = lap_number

    @staticmethod
    def millisec_to_sec(time: int) -> float:
        if time < 0:
            return -1
        return round(time / 1000, 3)

    @staticmethod
    def sec_to_lap_string(time: float) -> str:
        "ss.000 -> m:ss.000"
        if time < 0:
            return ""
        return f"{int(time // 60)}:{time % 60:.3f}"

    @staticmethod
    def parse_lap(lap_dict: dict) -> "Lap":
        """
        {
            "carId": 1017,
            "driverIndex": 0,
            "isValidForBest": true,
            "lap_time": 110602, // milliseconds
            "splits": [
                34462,
                40940,
                35200
            ]
        },
        """
        car_id: int = lap_dict[Lap.car_id_json]
        driver_index: int = lap_dict[Lap.driver_index_json]
        is_valid_for_best: bool = lap_dict[Lap.is_valid_for_best_json]
        lap_time: int = lap_dict[Lap.lap_time_json]
        splits: list[int] = [split for split in lap_dict[Lap.splits_json]]
        return Lap(car_id, driver_index, is_valid_for_best, lap_time, splits)

    def insert_into_lap_table(
        self, sra_db_cursor: MySQLCursorAbstract, session: "Session"
    ) -> None:
        """
        +-------------------+-------------+------+-----+---------+-------+
        | Field             | Type        | Null | Key | Default | Extra |
        +-------------------+-------------+------+-----+---------+-------+
        | session_file      | varchar(18) | NO   | PRI | NULL    |       |
        | server_number     | int(11)     | NO   | PRI | NULL    |       |
        | car_id            | varchar(4)  | NO   | PRI | NULL    |       |
        | driver_id         | varchar(18) | NO   | PRI | NULL    |       |
        | is_valid_for_best | tinyint(1)  | YES  |     | NULL    |       |
        | lap_time           | int(11)     | YES  |     | NULL    |       |
        | split1            | int(11)     | YES  |     | NULL    |       |
        | split2            | int(11)     | YES  |     | NULL    |       |
        | split3            | int(11)     | YES  |     | NULL    |       |
        | lap_number        | int(11)     | NO   | PRI | NULL    |       |
        +-------------------+-------------+------+-----+---------+-------+
        """
        driver: LeaderboardDriver = session.session_result.car_results_dict[
            self.car_id
        ].drivers[self.driver_index]
        # ensuring there are 3 splits, it's possible there aren't if the lap is not completed(?)
        self.splits += [-1] * (3 - len(self.splits))
        insert_query = f"""
            INSERT IGNORE INTO car_laps (
                session_file, server_number, car_id, driver_id, is_valid_for_best, 
                lap_time, split1, split2, split3, lap_number
            ) VALUES (
                '{session.session_file}', {session.server_number}, '{self.car_id}', '{driver.driver_id}', {self.is_valid_for_best},
                {self.lap_time}, {self.splits[0]}, {self.splits[1]}, {self.splits[2]}, {self.lap_number}
            );
        """
        sra_db_cursor.execute(insert_query)


class LeaderboardDriver:
    first_name_json = "firstName"
    last_name_json = "lastName"
    driver_id_json = "playerId"
    short_name_json = "shortName"

    @dataclass
    class ProcessedRaceLaps:
        first_percentile_lap: int
        twentyfifth_percentile_lap: int
        fiftieth_percentile_lap: int
        seventyfifth_percentile_lap: int
        # total_time is the sum of all laps, different from car.total_time
        # which is time from green flag to checkered flag
        total_time: int

        @property
        def average_lap(self) -> float:
            return np.mean(
                [
                    self.twentyfifth_percentile_lap,
                    self.fiftieth_percentile_lap,
                    self.seventyfifth_percentile_lap,
                ]
            )

    @dataclass
    class ProcessedQualiLaps:
        best_lap_number: int

        def theoretical_best_lap(
            self, best_split1: int, best_split2: int, best_split3: int
        ) -> int:
            return best_split1 + best_split2 + best_split3

    def __init__(
        self,
        first_name: str,
        last_name: str,
        driver_id: str,
        short_name: str,
    ) -> None:
        self.first_name = first_name
        self.last_name = last_name
        self.driver_id = driver_id
        self.short_name = short_name
        self.laps: list[Lap] = []

    # car
    @property
    def car(self) -> "LeaderboardCar":
        return self.__car

    @car.setter
    def car(self, car: "LeaderboardCar") -> None:
        self.__car = car

    # avg percent diff
    @property
    def avg_percent_diff(self) -> float:
        return self.__avg_percent_diff

    @avg_percent_diff.setter
    def avg_percent_diff(self, avg_percent_diff: float) -> None:
        self.__avg_percent_diff = avg_percent_diff

    # pace vs field
    @property
    def pace_vs_field(self) -> float:
        return self.__pace_vs_field

    @pace_vs_field.setter
    def pace_vs_field(self, pace_vs_field: float) -> None:
        self.__pace_vs_field = pace_vs_field

    # processed race laps
    @property
    def processed_race_laps(self) -> "LeaderboardDriver.ProcessedRaceLaps":
        return self.__processed_race_laps

    @processed_race_laps.setter
    def processed_race_laps(
        self, processed_laps: "LeaderboardDriver.ProcessedRaceLaps"
    ) -> None:
        self.__processed_race_laps = processed_laps

    # processed quali laps
    @property
    def processed_quali_laps(self) -> "LeaderboardDriver.ProcessedQualiLaps":
        return self.__processed_quali_laps

    @processed_quali_laps.setter
    def processed_quali_laps(
        self, processed_laps: "LeaderboardDriver.ProcessedQualiLaps"
    ) -> None:
        self.__processed_quali_laps = processed_laps

    @staticmethod
    def parse_driver(driver_dict: dict) -> "LeaderboardDriver":
        """
        {
            "firstName": "Bryan",
            "lastName": "Anderson",
            "playerId": "S76561197993701529",
            "shortName": "AND"
        }
        """
        first_name: str = driver_dict[LeaderboardDriver.first_name_json]
        last_name: str = driver_dict[LeaderboardDriver.last_name_json]
        driver_id: str = driver_dict[LeaderboardDriver.driver_id_json]
        short_name: str = driver_dict[LeaderboardDriver.short_name_json]
        return LeaderboardDriver(first_name, last_name, driver_id, short_name)

    def process_quali_laps(self):
        best_lap_number = -1
        for i, lap in enumerate(self.laps):
            if lap.lap_time == self.car.leaderboard_line.timing.best_lap:
                best_lap_number = i + 1
        processed_quali_laps: LeaderboardDriver.ProcessedQualiLaps = (
            LeaderboardDriver.ProcessedQualiLaps(
                best_lap_number=best_lap_number,
            )
        )
        self.processed_quali_laps = processed_quali_laps

    def process_race_laps(self):
        lap_times = []
        total_time = 0
        for lap in self.laps:
            lap_times.append(lap.lap_time)
            total_time += lap.lap_time

        percentiles = np.percentile(lap_times, [1, 25, 50, 75])
        (
            first_percentile_lap,
            twentyfifth_percentile_lap,
            fiftieth_percentile_lap,
            seventyfifth_percentile_lap,
        ) = percentiles

        processed_race_laps: LeaderboardDriver.ProcessedRaceLaps = (
            LeaderboardDriver.ProcessedRaceLaps(
                first_percentile_lap=first_percentile_lap,
                twentyfifth_percentile_lap=twentyfifth_percentile_lap,
                fiftieth_percentile_lap=fiftieth_percentile_lap,
                seventyfifth_percentile_lap=seventyfifth_percentile_lap,
                total_time=total_time,
            )
        )
        self.processed_race_laps = processed_race_laps

    def insert_into_driver_qualis_processed(
        self, session: "Session", sra_db_cursor: MySQLCursorAbstract
    ) -> None:
        """
        +----------------------+-------------+------+-----+---------+-------+
        | Field                | Type        | Null | Key | Default | Extra |
        +----------------------+-------------+------+-----+---------+-------+
        | session_file         | varchar(18) | NO   | PRI | NULL    |       |
        | server_number        | int(11)     | NO   | PRI | NULL    |       |
        | car_id               | varchar(4)  | NO   | PRI | NULL    |       |
        | driver_id            | varchar(18) | NO   | PRI | NULL    |       |
        | multiple_drivers     | tinyint(1)  | YES  |     | NULL    |       |
        | best_lap_number      | int(11)     | YES  |     | NULL    |       |
        | best_lap_time        | int(11)     | YES  |     | NULL    |       |
        | best_split1          | int(11)     | YES  |     | NULL    |       |
        | best_split2          | int(11)     | YES  |     | NULL    |       |
        | best_split3          | int(11)     | YES  |     | NULL    |       |
        | theoretical_best_lap | int(11)     | YES  |     | NULL    |       |
        | finish_position      | int(11)     | YES  |     | NULL    |       |
        | pace_vs_field        | float       | YES  |     | NULL    |       |
        +----------------------+-------------+------+-----+---------+-------+
        """
        if not self.laps or self.avg_percent_diff == -1 or np.isnan(self.pace_vs_field):
            return

        best_lap_time = self.car.leaderboard_line.timing.best_lap
        best_split1 = self.car.leaderboard_line.timing.best_splits[0]
        best_split2 = self.car.leaderboard_line.timing.best_splits[1]
        best_split3 = self.car.leaderboard_line.timing.best_splits[2]
        theoretical_best_lap = self.processed_quali_laps.theoretical_best_lap(
            best_split1, best_split2, best_split3
        )

        insert_query = f"""
            INSERT INTO driver_qualis_processed (
                session_file, server_number, car_id, driver_id, multiple_drivers,
                best_lap_number, best_lap_time,
                best_split1, best_split2, best_split3, theoretical_best_lap, finish_position, pace_vs_field
            ) VALUES (
                '{session.session_file}', {session.server_number}, '{self.car.car_id}', '{self.driver_id}', {len(self.car.drivers) > 1},
                {self.processed_quali_laps.best_lap_number}, {best_lap_time},
                {best_split1}, {best_split2}, {best_split3}, {theoretical_best_lap}, {self.car.leaderboard_line.finish_position}, {self.pace_vs_field}
            ) ON DUPLICATE KEY UPDATE
                pace_vs_field=VALUES(pace_vs_field);
        """
        sra_db_cursor.execute(insert_query)

    def insert_into_driver_races_processed(
        self, session: "Session", sra_db_cursor: MySQLCursorAbstract
    ) -> None:
        """
        +-------------------+-------------+------+-----+---------+-------+
        | Field             | Type        | Null | Key | Default | Extra |
        +-------------------+-------------+------+-----+---------+-------+
        | session_file      | varchar(18) | NO   | PRI | NULL    |       |
        | server_number     | int(11)     | NO   | PRI | NULL    |       |
        | car_id            | varchar(4)  | NO   | PRI | NULL    |       |
        | driver_id         | varchar(18) | NO   | PRI | NULL    |       |
        | multiple_drivers  | tinyint(1)  | YES  |     | NULL    |       |
        | 1_percentile_lap  | int(11)     | YES  |     | NULL    |       |
        | 25_percentile_lap | int(11)     | YES  |     | NULL    |       |
        | 50_percentile_lap | int(11)     | YES  |     | NULL    |       |
        | 75_percentile_lap | int(11)     | YES  |     | NULL    |       |
        | total_time        | int(11)     | YES  |     | NULL    |       |
        | start_position    | int(11)     | YES  |     | NULL    |       |
        | finish_position   | int(11)     | YES  |     | NULL    |       |
        | pace_vs_field     | float       | YES  |     | NULL    |       |
        +-------------------+-------------+------+-----+---------+-------+
        """
        if not self.laps or np.isnan(self.pace_vs_field):
            return

        insert_query = f"""
            INSERT INTO driver_races_processed (
                session_file, server_number, car_id, driver_id, multiple_drivers, 
                1_percentile_lap, 25_percentile_lap, 50_percentile_lap, 75_percentile_lap, total_time,
                start_position, finish_position, pace_vs_field
            ) VALUES (
                '{session.session_file}', {session.server_number}, '{self.car.car_id}', '{self.driver_id}', {len(self.car.drivers) > 1},
                {self.processed_race_laps.first_percentile_lap}, {self.processed_race_laps.twentyfifth_percentile_lap}, {self.processed_race_laps.fiftieth_percentile_lap}, {self.processed_race_laps.seventyfifth_percentile_lap}, {self.processed_race_laps.total_time},
                -1, {self.car.leaderboard_line.finish_position}, {self.pace_vs_field}
            ) ON DUPLICATE KEY UPDATE
                pace_vs_field=VALUES(pace_vs_field);
        """
        sra_db_cursor.execute(insert_query)


class LeaderboardCar:
    car_id_json = "carId"
    car_model_json = "carModel"
    car_group_json = "carGroup"
    car_guid_json = "carGuid"
    team_guid_json = "teamGuid"
    cup_category_json = "cupCategory"
    drivers_json = "drivers"
    nationality_json = "nationality"
    car_number_json = "raceNumber"
    team_name_json = "teamName"

    def __init__(
        self,
        car_id: int,
        car_model: int,
        car_group: str,
        car_guid: int,
        team_guid: int,
        cup_category: int,
        drivers: list["LeaderboardDriver"],
        nationality: int,
        race_number: int,
        team_name: str,
    ) -> None:
        self.car_id = car_id
        self.car_model = car_model
        self.car_group = car_group
        self.car_guid = car_guid
        self.team_guid = team_guid
        self.cup_category = cup_category
        self.drivers = drivers
        self.nationality = nationality
        self.car_number = race_number
        self.team_name = team_name
        self.laps: list[Lap] = []

    @property
    def leaderboard_line(self) -> "LeaderBoardLine":
        return self.__leaderboard_line

    @leaderboard_line.setter
    def leaderboard_line(self, leaderboard_line: "LeaderBoardLine") -> None:
        self.__leaderboard_line = leaderboard_line

    @staticmethod
    def parse_car(car_dict: dict) -> "LeaderboardCar":
        """
        {
            "carId": 1002,
            "carModel": 35,
            "carGroup": "GT3",
            "carGuid": -1,
            "teamGuid": -1,
            "cupCategory": 0,
            "drivers": [
                {
                "firstName": "Bryan",
                "lastName": "Anderson",
                "playerId": "S76561197993701529",
                "shortName": "AND"
                }
            ],
            "nationality": 0,
            "raceNumber": 502,
            "teamName": ""
        }
        """
        car_id: int = car_dict[LeaderboardCar.car_id_json]
        car_model: int = car_dict[LeaderboardCar.car_model_json]
        car_group: str = car_dict[LeaderboardCar.car_group_json]
        car_guid: int = car_dict[LeaderboardCar.car_guid_json]
        team_guid: int = car_dict[LeaderboardCar.team_guid_json]
        cup_category: int = car_dict[LeaderboardCar.cup_category_json]
        drivers: list[LeaderboardDriver] = [
            LeaderboardDriver.parse_driver(driver)
            for driver in car_dict[LeaderboardCar.drivers_json]
        ]
        nationality: int = car_dict[LeaderboardCar.nationality_json]
        race_number: int = car_dict[LeaderboardCar.car_number_json]
        team_name: str = car_dict[LeaderboardCar.team_name_json]
        return LeaderboardCar(
            car_id=car_id,
            car_model=car_model,
            car_group=car_group,
            car_guid=car_guid,
            team_guid=team_guid,
            cup_category=cup_category,
            drivers=drivers,
            nationality=nationality,
            race_number=race_number,
            team_name=team_name,
        )


class LeaderboardTiming:
    best_lap_json = "bestLap"
    best_splits_json = "bestSplits"
    lap_count_json = "lapCount"
    last_lap_json = "lastLap"
    last_split_id_json = "lastSplitId"
    last_splits_json = "lastSplits"
    total_time_json = "totalTime"

    def __init__(
        self,
        best_lap: int,
        best_splits: list[int],
        lap_count: int,
        last_lap: int,
        last_split_id: int,
        last_splits: list[int],
        total_time: int,
    ) -> None:
        self.best_lap = best_lap
        self.best_splits = best_splits
        self.lap_count = lap_count
        self.last_lap = last_lap
        self.last_split_id = last_split_id
        self.last_splits = last_splits
        self.total_time = total_time

    @staticmethod
    def parse_timing(timing_dict: dict) -> "LeaderboardTiming":
        """
        "timing": {
            "bestLap": 104760,
            "bestSplits": [
                29520,
                40257,
                34977
            ],
            "lapCount": 16,
            "lastLap": 105527,
            "lastSplitId": 0,
            "lastSplits": [
                29545,
                40402,
                35580
            ],
            "totalTime": 2626084
        }
        """
        best_lap: int = timing_dict[LeaderboardTiming.best_lap_json]
        best_splits: list[int] = [
            split for split in timing_dict[LeaderboardTiming.best_splits_json]
        ]
        lap_count: int = timing_dict[LeaderboardTiming.lap_count_json]
        last_lap: int = timing_dict[LeaderboardTiming.last_lap_json]
        last_split_id: int = timing_dict[LeaderboardTiming.last_split_id_json]
        last_splits: list[int] = [
            split for split in timing_dict[LeaderboardTiming.last_splits_json]
        ]
        total_time: int = timing_dict[LeaderboardTiming.total_time_json]
        return LeaderboardTiming(
            best_lap=best_lap,
            best_splits=best_splits,
            lap_count=lap_count,
            last_lap=last_lap,
            last_split_id=last_split_id,
            last_splits=last_splits,
            total_time=total_time,
        )


class LeaderBoardLine:
    car_json = "car"
    current_driver_json = "currentDriver"
    current_driver_index_json = "currentDriverIndex"
    driver_total_times_json = "driverTotalTimes"
    missing_mandatory_pitstop_json = "missingMandatoryPitstop"
    timing_json = "timing"

    def __init__(
        self,
        car: "LeaderboardCar",
        current_driver: "LeaderboardDriver",
        current_driver_index: int,
        driver_total_times: list[int],
        missing_mandatory_pitstop: int,
        timing: "LeaderboardTiming",
        finish_position: int,
    ) -> None:
        self.car = car
        self.current_driver = current_driver
        self.current_driver_index = current_driver_index
        self.driver_total_times = driver_total_times
        self.missing_mandatory_pitstop = missing_mandatory_pitstop
        self.timing = timing
        self.finish_position = finish_position

    @staticmethod
    def parse_leaderboard_line(
        leaderboard_line_dict: dict, finish_position: int
    ) -> "LeaderBoardLine":
        """
        {
            "car": {
                "carId": 1002,
                "carModel": 35,
                "carGroup": "GT3",
                "carGuid": -1,
                "teamGuid": -1,
                "cupCategory": 0,
                "drivers": [
                    {
                    "firstName": "Bryan",
                    "lastName": "Anderson",
                    "playerId": "S76561197993701529",
                    "shortName": "AND"
                    }
                ],
                "nationality": 0,
                "raceNumber": 502,
                "teamName": ""
            },
            "currentDriver": {
                "firstName": "Bryan",
                "lastName": "Anderson",
                "playerId": "S76561197993701529",
                "shortName": "AND"
            },
            "currentDriverIndex": 0,
            "driverTotalTimes": [
                1838284.125
            ],
            "missingMandatoryPitstop": 0,
            "timing": {
                "bestLap": 104760,
                "bestSplits": [
                    29520,
                    40257,
                    34977
                ],
                "lapCount": 16,
                "lastLap": 105527,
                "lastSplitId": 0,
                "lastSplits": [
                    29545,
                    40402,
                    35580
                ],
                "totalTime": 2626084
            }
        }
        """
        car: LeaderboardCar = LeaderboardCar.parse_car(
            leaderboard_line_dict[LeaderBoardLine.car_json]
        )
        current_driver: LeaderboardDriver = LeaderboardDriver.parse_driver(
            leaderboard_line_dict[LeaderBoardLine.current_driver_json]
        )
        current_driver_index: int = leaderboard_line_dict[
            LeaderBoardLine.current_driver_index_json
        ]
        driver_total_times: list[int] = leaderboard_line_dict[
            LeaderBoardLine.driver_total_times_json
        ]
        missing_mandatory_pitstop: int = leaderboard_line_dict[
            LeaderBoardLine.missing_mandatory_pitstop_json
        ]
        timing: LeaderboardTiming = LeaderboardTiming.parse_timing(
            leaderboard_line_dict[LeaderBoardLine.timing_json]
        )
        return LeaderBoardLine(
            car=car,
            current_driver=current_driver,
            current_driver_index=current_driver_index,
            driver_total_times=driver_total_times,
            missing_mandatory_pitstop=missing_mandatory_pitstop,
            timing=timing,
            finish_position=finish_position,
        )


class SessionResult:
    best_splits_json = "bestSplits"
    best_lap_json = "bestlap"
    is_wet_session_json = "isWetSession"
    leaderboard_lines_json = "leaderBoardLines"
    session_type_json = "type"  # 0 = quali, 1 = race, 2 = practice(?)

    def __init__(
        self,
        best_splits: list[int],
        best_lap: int,
        is_wet_session: bool,
        leaderboard_lines: list["LeaderBoardLine"],
        session_type: str,
        car_results_dict: dict[int, "LeaderboardCar"],
        driver_results_dict: dict[str, "LeaderboardDriver"],
    ) -> None:
        self.best_splits = best_splits
        self.best_lap = best_lap
        self.is_wet_session = is_wet_session
        self.leaderboard_lines = leaderboard_lines
        self.session_type = session_type
        self.car_results_dict = car_results_dict
        self.driver_results_dict = driver_results_dict

    @staticmethod
    def parse_session_result(session_result_dict: dict) -> "SessionResult":
        """
        {
            "bestSplits": [
                29520,
                40257,
                34977
            ],
            "bestLap": 104760,
            "isWetSession": 0,
            "leaderBoardLines": [...],
            "type": "0"
        }
        """
        # best splits
        best_splits: list[int] = [
            split for split in session_result_dict[SessionResult.best_splits_json]
        ]

        # best lap
        best_lap: int = session_result_dict[SessionResult.best_lap_json]

        # is wet session
        is_wet_session: bool = (
            True if session_result_dict[SessionResult.is_wet_session_json] else 0
        )

        # leaderboard lines
        # we're making a car_results_dict to be able to identify get leaderboard
        # details by car other than by position (the natural order of leaderboard_lines)
        leaderboard_lines: list[LeaderBoardLine] = []
        car_results_dict: dict[int, LeaderboardCar] = {}
        driver_results_dict: dict[str, LeaderboardDriver] = {}
        for i, line in enumerate(
            session_result_dict[SessionResult.leaderboard_lines_json]
        ):
            leaderboard_line = LeaderBoardLine.parse_leaderboard_line(
                line, finish_position=i + 1
            )
            leaderboard_lines.append(leaderboard_line)
            leaderboard_line.car.leaderboard_line = leaderboard_line
            car_results_dict[leaderboard_line.car.car_id] = leaderboard_line.car
            for driver in leaderboard_line.car.drivers:
                driver.car = leaderboard_line.car
                driver_results_dict[driver.driver_id] = driver

        # session type
        session_type: str = session_result_dict[SessionResult.session_type_json]

        return SessionResult(
            best_splits=best_splits,
            best_lap=best_lap,
            is_wet_session=is_wet_session,
            leaderboard_lines=leaderboard_lines,
            session_type=session_type,
            car_results_dict=car_results_dict,
            driver_results_dict=driver_results_dict,
        )

    def insert_into_car_results_table(
        self, session: "Session", sra_db_cursor: MySQLCursorAbstract
    ) -> None:
        """
        +-----------------+--------------+------+-----+---------+-------+
        | Field           | Type         | Null | Key | Default | Extra |
        +-----------------+--------------+------+-----+---------+-------+
        | session_file    | varchar(20)  | NO   | PRI | NULL    |       |
        | server_number   | int(11)      | NO   | PRI | NULL    |       |
        | session_type    | varchar(2)   | YES  |     | NULL    |       |
        | car_id          | varchar(4)   | NO   | PRI | NULL    |       |
        | car_model       | int(11)      | YES  |     | NULL    |       |
        | car_group       | varchar(255) | YES  |     | NULL    |       |
        | cup_category    | int(11)      | YES  |     | NULL    |       |
        | car_number      | int(11)      | YES  |     | NULL    |       |
        | num_drivers     | int(11)      | YES  |     | NULL    |       |
        | missing_pit     | tinyint(1)   | YES  |     | NULL    |       |
        | best_lap        | int(11)      | YES  |     | NULL    |       |
        | best_split1     | int(11)      | YES  |     | NULL    |       |
        | best_split2     | int(11)      | YES  |     | NULL    |       |
        | best_split3     | int(11)      | YES  |     | NULL    |       |
        | lap_count       | int(11)      | YES  |     | NULL    |       |
        | total_time      | int(11)      | YES  |     | NULL    |       |
        | finish_position | int(11)      | YES  |     | NULL    |       |
        +-----------------+--------------+------+-----+---------+-------+
        """
        for line in self.leaderboard_lines:
            insert_query = f"""
                INSERT IGNORE INTO car_results (
                    session_file, server_number, session_type, car_id,
                    car_model, car_group, cup_category, car_number,
                    num_drivers, missing_pit, best_lap, best_split1,
                    best_split2, best_split3, lap_count, total_time, 
                    finish_position
                ) VALUES (
                    '{session.session_file}', {session.server_number}, '{session.session_type}', '{line.car.car_id}',
                    {line.car.car_model}, '{line.car.car_group}', {line.car.cup_category}, {line.car.car_number},
                    {len(line.car.drivers)}, {line.missing_mandatory_pitstop}, {self.best_lap}, {self.best_splits[0]},
                    {self.best_splits[1]}, {self.best_splits[2]}, {line.timing.lap_count}, {line.timing.total_time},
                    {line.finish_position}
                );
            """
            sra_db_cursor.execute(insert_query)

    def insert_into_driver_sessions_table(
        self, session: "Session", sra_db_cursor: MySQLCursorAbstract
    ) -> None:
        """
        +---------------+-------------+------+-----+---------+-------+
        | Field         | Type        | Null | Key | Default | Extra |
        +---------------+-------------+------+-----+---------+-------+
        | session_file  | varchar(18) | NO   | PRI | NULL    |       |
        | server_number | int(11)     | NO   | PRI | NULL    |       |
        | car_id        | varchar(4)  | NO   | PRI | NULL    |       |
        | driver1_id    | varchar(18) | NO   | PRI | NULL    |       |
        | driver2_id    | varchar(18) | NO   | PRI | NULL    |       |
        | driver3_id    | varchar(18) | NO   | PRI | NULL    |       |
        | driver4_id    | varchar(18) | NO   | PRI | NULL    |       |
        | driver5_id    | varchar(18) | NO   | PRI | NULL    |       |
        | driver6_id    | varchar(18) | NO   | PRI | NULL    |       |
        +---------------+-------------+------+-----+---------+-------+
        """
        for line in self.leaderboard_lines:
            driver_ids = [driver.driver_id for driver in line.car.drivers]
            driver_ids += [""] * (6 - len(driver_ids))
            insert_query = f"""
                INSERT IGNORE INTO driver_sessions (
                    session_file, server_number, car_id, driver1_id, driver2_id, driver3_id, driver4_id, driver5_id, driver6_id
                ) VALUES (
                    '{session.session_file}', {session.server_number}, '{line.car.car_id}', '{driver_ids[0]}', '{driver_ids[1]}', '{driver_ids[2]}', '{driver_ids[3]}', '{driver_ids[4]}', '{driver_ids[5]}'
                );
            """
            sra_db_cursor.execute(insert_query)

    def insert_into_drivers_table(self, sra_db_cursor: MySQLCursorAbstract) -> None:
        """
        +------------+-------------+------+-----+---------+-------+
        | Field      | Type        | Null | Key | Default | Extra |
        +------------+-------------+------+-----+---------+-------+
        | driver_id  | varchar(18) | NO   | PRI |         |       |
        | first_name | varchar(30) | YES  |     | NULL    |       |
        | last_name  | varchar(30) | YES  |     | NULL    |       |
        | short_name | varchar(3)  | YES  |     | NULL    |       |
        +------------+-------------+------+-----+---------+-------+
        """
        for line in self.leaderboard_lines:
            for driver in line.car.drivers:
                # we ignore because we're inserting newest to oldest
                insert_query = f"""
                    INSERT INTO drivers (
                        driver_id, first_name, last_name, short_name
                    ) VALUES (
                        '{MySqlDatabase.handle_bad_string(driver.driver_id)}', '{MySqlDatabase.handle_bad_string(driver.first_name)}',
                        '{MySqlDatabase.handle_bad_string(driver.last_name)}', '{MySqlDatabase.handle_bad_string(driver.short_name)}'
                    ) ON DUPLICATE KEY UPDATE 
                        first_name=VALUES(first_name), 
                        last_name=VALUES(last_name), 
                        short_name=VALUES(short_name);
                """
                sra_db_cursor.execute(insert_query)


class Session:
    laps_json = "laps"
    penalties_json = "penalties"
    post_race_penalties_json = "post_race_penalties"
    session_index_json = "sessionIndex"
    race_weekend_index_json = "raceWeekendIndex"
    session_result_json = "sessionResult"
    session_type_json = "sessionType"
    track_name_json = "trackName"
    server_name_json = "serverName"
    meta_data_json = "metaData"
    date_json = "Date"
    session_file_json = "SessionFile"
    server_number_json = "serverNumber"

    """
    example penalty
    {
    "carId": 1012,
    "clearedInLap": 3,
    "driverIndex": 0,
    "penalty": "StopAndGo_30",
    "penaltyValue": 2,
    "reason": "PitSpeeding",
    "violationInLap": 2
    },
    """

    def __init__(
        self,
        laps: list["Lap"],
        penalties: list[dict],  # TODO
        post_race_penalties: list[dict],  # TODO
        session_index: int,
        race_weekend_index: int,
        session_result: SessionResult,
        session_type: str,
        track_name: str,
        server_name: str,
        meta_data: str,
        finish_time: datetime,
        session_file: str,
        server_number: int,
    ) -> None:
        self.laps = laps
        self.penalties = penalties
        self.post_race_penalties = post_race_penalties
        self.session_index = session_index
        self.race_weekend_index = race_weekend_index
        self.session_result = session_result
        self.session_type = session_type
        self.track_name = track_name
        self.server_name = server_name
        self.meta_data = meta_data
        self.finish_time = finish_time
        self.session_file = session_file
        self.server_number = server_number

    @staticmethod
    def parse_session(session_result_dict: dict) -> "Session":
        """
        {
            "laps": [...],
            "penalties": [...],
            "post_race_penalties": null // FIXME list of unknown type
            "sessionIndex": 0,
            "raceWeekendIndex": 0,
            "sessionResult": {...},
            "sessionType": "R",
            "trackName": "barcelona",
            "serverName": "",
            "metaData": "custom_race:db72fd90-2a45-433a-a574-282acdbf9b4a",
            "Date": "2021-12-29T07:49:11Z",
            "SessionFile": "211229_074911_R",
            "server_number": 2
        }
        """
        laps: list[Lap] = [
            Lap.parse_lap(lap) for lap in session_result_dict[Session.laps_json]
        ]
        penalties = session_result_dict[Session.penalties_json]
        post_race_penalties = session_result_dict[Session.post_race_penalties_json]
        race_weekend_index: int = session_result_dict[Session.race_weekend_index_json]
        session_index: int = session_result_dict[Session.session_index_json]
        session_result: SessionResult = SessionResult.parse_session_result(
            session_result_dict[Session.session_result_json]
        )
        session_type: str = session_result_dict[Session.session_type_json]
        track_name: str = session_result_dict[Session.track_name_json]
        server_name: str = session_result_dict[Session.server_name_json]
        meta_data: str = session_result_dict[Session.meta_data_json]
        date: datetime = datetime.strptime(
            session_result_dict[Session.date_json], "%Y-%m-%dT%H:%M:%SZ"
        )
        session_file: str = session_result_dict[Session.session_file_json]
        server_number: int = session_result_dict[Session.server_number_json]

        return Session(
            laps=laps,
            penalties=penalties,
            post_race_penalties=post_race_penalties,
            session_index=session_index,
            race_weekend_index=race_weekend_index,
            session_result=session_result,
            session_type=session_type,
            track_name=track_name,
            server_name=server_name,
            meta_data=meta_data,
            finish_time=date,
            session_file=session_file,
            server_number=server_number,
        )

    def evaluate_drivers(self, session_type: str):
        min_avg_percent_diff = float("inf")
        max_avg_percent_diff = float("-inf")
        for i_driver in self.session_result.driver_results_dict.values():
            i_driver.avg_percent_diff = -1
            if not i_driver.laps:
                continue

            if i_driver.car.leaderboard_line.timing.best_lap in {-1, 2147483647}:
                continue

            percent_diffs = []
            for j_driver in self.session_result.driver_results_dict.values():
                if not j_driver.laps:
                    continue

                if j_driver.car.leaderboard_line.timing.best_lap in {-1, 2147483647}:
                    continue

                if i_driver == j_driver:
                    continue

                if session_type == "R":
                    percent_diff = (
                        i_driver.processed_race_laps.average_lap
                        / j_driver.processed_race_laps.average_lap
                    )
                elif session_type == "Q":
                    percent_diff = (
                        i_driver.car.leaderboard_line.timing.best_lap
                        / j_driver.car.leaderboard_line.timing.best_lap
                    )
                percent_diffs.append(percent_diff)
            i_driver.avg_percent_diff = np.mean(percent_diffs)

            min_avg_percent_diff = min(min_avg_percent_diff, i_driver.avg_percent_diff)
            max_avg_percent_diff = max(max_avg_percent_diff, i_driver.avg_percent_diff)

        avg_percent_diff_range = max_avg_percent_diff - min_avg_percent_diff
        for driver in self.session_result.driver_results_dict.values():
            if not driver.laps:
                continue
            if driver.avg_percent_diff == -1:
                continue
            driver.pace_vs_field = (
                driver.avg_percent_diff - min_avg_percent_diff
            ) / avg_percent_diff_range

    def insert_into_session_table(self, sra_db_cursor: MySQLCursorAbstract) -> None:
        """
        +--------------------+--------------+------+-----+---------+-------+
        | Field              | Type         | Null | Key | Default | Extra |
        +--------------------+--------------+------+-----+---------+-------+
        | session_file       | varchar(20)  | NO   | PRI | NULL    |       |
        | finish_time        | timestamp    | YES  |     | NULL    |       |
        | track_name         | varchar(255) | YES  |     | NULL    |       |
        | session_type       | varchar(2)   | YES  |     | NULL    |       |
        | session_index      | int(11)      | YES  |     | NULL    |       |
        | race_weekend_index | int(11)      | YES  |     | NULL    |       |
        | server_name        | varchar(255) | YES  |     | NULL    |       |
        | server_number      | int(11)      | NO   | PRI | NULL    |       |
        | meta_data          | varchar(255) | YES  |     | NULL    |       |
        | is_wet_session     | tinyint(1)   | YES  |     | NULL    |       |
        +--------------------+--------------+------+-----+---------+-------+
        """

        insert_query = f"""
            INSERT IGNORE INTO sessions (
                session_file, finish_time, track_name, 
                session_type, session_index, 
                race_weekend_index, 
                server_name, 
                server_number, meta_data, is_wet_session
            ) VALUES (
                '{self.session_file}', '{self.finish_time}', '{self.track_name}',
                '{self.session_type}', {self.session_index}, 
                {self.race_weekend_index}, 
                '{MySqlDatabase.handle_bad_string(self.server_name)}', 
                '{self.server_number}', '{self.meta_data}', '{self.session_result.is_wet_session}'
            );
        """
        sra_db_cursor.execute(insert_query)


if __name__ == "__main__":
    current_dir = os.path.dirname(__file__)
    ACCSM_dir = os.path.join(current_dir, "ACCSM")
    downloads_dir = os.path.join(ACCSM_dir, "downloads")
    races_dir = os.path.join(downloads_dir, "races")
    quali_dir = os.path.join(downloads_dir, "qualifyings")
    practices_dir = os.path.join(downloads_dir, "practices")
    # race_result_file = "Zolder_231213_224529_R_server4.json"
    # race_result_json = json.load(open(os.path.join(races_dir, race_result_file)))

    max_driver_name_length = 0
    max_driver_id_length = 0

    SQLAEngine = MySqlDatabase.create_engine()
    with SQLAEngine.connect() as connection:
        sessions_in_db = queries.get_session_files(connection)

    sra_db, sra_db_cursor = MySqlDatabase.connect_database("SRA")
    # parse leaderboard lines
    for session_dir in [practices_dir, races_dir, quali_dir][2:]:
        dir_by_date_modified = sorted(
            os.listdir(session_dir),
            key=lambda x: os.path.getmtime(os.path.join(session_dir, x)),
            reverse=True,
        )
        for i, session_file in enumerate(dir_by_date_modified):
            print(
                f"{i + 1}/{len(os.listdir(session_dir))} - Loading {session_file}...",
                end="",
            )

            server_number = session_file.split("server")[1].split(".")[0]
            with open(
                os.path.join(session_dir, session_file), "r", encoding="utf-8"
            ) as file:
                session_json = json.load(file)
            session_json["serverNumber"] = server_number

            session = Session.parse_session(session_json)
            dont_skip_insert = session.session_file not in sessions_in_db
            if not dont_skip_insert:
                print("skipping...", end="")

            for i, lap in enumerate(session.laps):
                if lap.car_id not in session.session_result.car_results_dict:
                    continue
                car = session.session_result.car_results_dict[lap.car_id]
                session.laps[i].lap_number = len(car.laps) + 1
                car.laps.append(lap)
                for i, driver in enumerate(car.drivers):
                    if lap.driver_index == i:
                        driver.laps.append(lap)
                        break
                if dont_skip_insert:
                    lap.insert_into_lap_table(sra_db_cursor, session)

            is_quali_session = session.session_type == "Q"
            is_race_session = session.session_type == "R"
            is_practice_session = session.session_type == "FP"

            for car in session.session_result.car_results_dict.values():
                for driver in car.drivers:
                    if True or dont_skip_insert:
                        if not driver.laps:
                            continue
                        if is_quali_session:
                            driver.process_quali_laps()
                        elif is_race_session:
                            driver.process_race_laps()

            if (True or dont_skip_insert) and not is_practice_session:
                session.evaluate_drivers(session.session_type)
                for driver in session.session_result.driver_results_dict.values():
                    if is_quali_session:
                        driver.insert_into_driver_qualis_processed(
                            session, sra_db_cursor
                        )
                    elif is_race_session:
                        driver.insert_into_driver_races_processed(
                            session, sra_db_cursor
                        )

            if dont_skip_insert:
                session.insert_into_session_table(sra_db_cursor)
                session.session_result.insert_into_car_results_table(
                    session, sra_db_cursor
                )
                session.session_result.insert_into_driver_sessions_table(
                    session, sra_db_cursor
                )
            # always try insert into drivers because drivers can change their names
            session.session_result.insert_into_drivers_table(sra_db_cursor)
            print("done")

            # Commit the transaction
            sra_db.commit()

    MySqlDatabase.close_connection(sra_db, sra_db_cursor)
    pass
