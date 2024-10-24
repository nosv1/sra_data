import json
import os
from datetime import datetime

import pandas as pd
from mysql.connector.abstracts import MySQLConnectionAbstract, MySQLCursorAbstract

from Database import Database


class Lap:
    car_id = "carId"
    driver_index = "driverIndex"
    is_valid_for_best = "isValidForBest"
    laptime = "laptime"
    splits = "splits"
    split1 = "split1"
    split2 = "split2"
    split3 = "split3"

    def __init__(
        self,
        car_id: int,
        driver_index: int,
        is_valid_for_best: bool,
        laptime: float,
        splits: list[float],
    ) -> None:
        self.car_id = car_id
        self.driver_index = driver_index
        self.is_valid_for_best = is_valid_for_best
        self.laptime = laptime
        self.splits = splits

    @staticmethod
    def time_to_float_m_ss_000(time: int) -> float:
        if time < 0:
            return -1

        # time is simply missing the symbols, so
        # 128577 should be 1:27.577 which is 87.577 seconds
        time: str = f"000000{time}"
        time, milliseconds = time[:-3], time[-3:]
        time, seconds = time[:-2], time[-2:]
        seconds = int(time) * 60 + int(seconds) + int(milliseconds) / 1000
        return seconds

    @staticmethod
    def time_to_float_ss_000(time: int) -> float:
        if time < 0:
            return -1
        # time is simply missing the symbols, so
        # 2626084 should be 2626.084 seconds
        time: str = f"000000{time}"
        time, milliseconds = time[:-3], time[-3:]
        seconds = int(time) + int(milliseconds) / 1000
        return seconds

    @staticmethod
    def parse_lap(lap_dict: dict) -> "Lap":
        """
        {
            "carId": 1017,
            "driverIndex": 0,
            "isValidForBest": true,
            "laptime": 110602, // mss000 -> 1:50.602
            "splits": [
                34462, // ss000 -> 34.462
                40940, // ss000 -> 40.940
                35200  // ss000 -> 35.200
            ]
        },
        // split times are in seconds but the last 3 digits are milliseconds, dissimilar to laptime which has minutes prepending seconds
        """
        car_id: int = lap_dict[Lap.car_id]
        driver_index: int = lap_dict[Lap.driver_index]
        is_valid_for_best: bool = lap_dict[Lap.is_valid_for_best]
        laptime: float = Lap.time_to_float_m_ss_000(lap_dict[Lap.laptime])
        splits: list[float] = [
            Lap.time_to_float_ss_000(split) for split in lap_dict[Lap.splits]
        ]
        return Lap(car_id, driver_index, is_valid_for_best, laptime, splits)

    @staticmethod
    def parse_laps(laps_dict: dict) -> pd.DataFrame:
        laps_df = pd.DataFrame(
            columns=[
                Lap.car_id,  # int
                Lap.driver_index,  # int
                Lap.is_valid_for_best,  # bool
                Lap.laptime,  # float
                Lap.split1,  # float
                Lap.split2,  # float
                Lap.split3,  # float
            ],
        )

        for lap_dict in laps_dict:
            lap = Lap.parse_lap(lap_dict)
            laps_df = pd.concat(
                [
                    laps_df,
                    pd.DataFrame(
                        {
                            Lap.car_id: [lap.car_id],
                            Lap.driver_index: [lap.driver_index],
                            Lap.is_valid_for_best: [lap.is_valid_for_best],
                            Lap.laptime: [lap.laptime],
                            Lap.split1: [lap.splits[0]],
                            Lap.split2: [lap.splits[1]],
                            Lap.split3: [lap.splits[2]],
                        }
                    ),
                ],
                ignore_index=True,
            )

        return laps_df


class LeaderboardDriver:
    first_name = "firstName"
    last_name = "lastName"
    player_id = "playerId"
    short_name = "shortName"

    def __init__(
        self,
        first_name: str,
        last_name: str,
        player_id: str,
        short_name: str,
    ) -> None:
        self.first_name = first_name
        self.last_name = last_name
        self.player_id = player_id
        self.short_name = short_name

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
        first_name: str = driver_dict[LeaderboardDriver.first_name]
        last_name: str = driver_dict[LeaderboardDriver.last_name]
        player_id: str = driver_dict[LeaderboardDriver.player_id]
        short_name: str = driver_dict[LeaderboardDriver.short_name]
        return LeaderboardDriver(first_name, last_name, player_id, short_name)


class LeaderboardCar:
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
    }
    """

    car_id = "carId"
    car_model = "carModel"
    car_group = "carGroup"
    car_guid = "carGuid"
    team_guid = "teamGuid"
    cup_category = "cupCategory"
    drivers = "drivers"

    def __init__(
        self,
        car_id: int,
        car_model: int,
        car_group: str,
        car_guid: int,
        team_guid: int,
        cup_category: int,
        drivers: list["LeaderboardDriver"],
    ) -> None:
        self.car_id = car_id
        self.car_model = car_model
        self.car_group = car_group
        self.car_guid = car_guid
        self.team_guid = team_guid
        self.cup_category = cup_category
        self.drivers = drivers

    @staticmethod
    def parse_car(car_dict: dict) -> "LeaderboardCar":
        car_id: int = car_dict[LeaderboardCar.car_id]
        car_model: int = car_dict[LeaderboardCar.car_model]
        car_group: str = car_dict[LeaderboardCar.car_group]
        car_guid: int = car_dict[LeaderboardCar.car_guid]
        team_guid: int = car_dict[LeaderboardCar.team_guid]
        cup_category: int = car_dict[LeaderboardCar.cup_category]
        drivers: list[LeaderboardDriver] = [
            LeaderboardDriver.parse_driver(driver)
            for driver in car_dict[LeaderboardCar.drivers]
        ]
        return LeaderboardCar(
            car_id=car_id,
            car_model=car_model,
            car_group=car_group,
            car_guid=car_guid,
            team_guid=team_guid,
            cup_category=cup_category,
            drivers=drivers,
        )


class LeaderboardTiming:
    best_lap = "bestLap"
    best_splits = "bestSplits"
    lap_count = "lapCount"
    last_lap = "lastLap"
    last_split_id = "lastSplitId"
    last_splits = "lastSplits"
    total_time = "totalTime"

    def __init__(
        self,
        best_lap: float,
        best_splits: list[float],
        lap_count: int,
        last_lap: float,
        last_split_id: int,
        last_splits: list[float],
        total_time: float,
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
        best_lap: float = Lap.time_to_float_m_ss_000(
            timing_dict[LeaderboardTiming.best_lap]
        )
        best_splits: list[float] = [
            Lap.time_to_float_ss_000(split)
            for split in timing_dict[LeaderboardTiming.best_splits]
        ]
        lap_count: int = timing_dict[LeaderboardTiming.lap_count]
        last_lap: float = Lap.time_to_float_m_ss_000(
            timing_dict[LeaderboardTiming.last_lap]
        )
        last_split_id: int = timing_dict[LeaderboardTiming.last_split_id]
        last_splits: list[float] = [
            Lap.time_to_float_ss_000(split)
            for split in timing_dict[LeaderboardTiming.last_splits]
        ]
        total_time: float = Lap.time_to_float_ss_000(
            timing_dict[LeaderboardTiming.total_time]
        )
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
    car = "car"
    current_driver = "currentDriver"
    current_driver_index = "currentDriverIndex"
    driver_total_times = "driverTotalTimes"
    missing_mandatory_pitstop = "missingMandatoryPitstop"
    timing = "timing"

    def __init__(
        self,
        car: "LeaderboardCar",
        current_driver: "LeaderboardDriver",
        current_driver_index: int,
        driver_total_times: list[float],
        missing_mandatory_pitstop: int,
        timing: "LeaderboardTiming",
    ) -> None:
        self.car = car
        self.current_driver = current_driver
        self.current_driver_index = current_driver_index
        self.driver_total_times = driver_total_times
        self.missing_mandatory_pitstop = missing_mandatory_pitstop
        self.timing = timing

    @staticmethod
    def parse_leaderboard_line(leaderboard_line_dict: dict) -> "LeaderBoardLine":
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
            leaderboard_line_dict[LeaderBoardLine.car]
        )
        current_driver: LeaderboardDriver = LeaderboardDriver.parse_driver(
            leaderboard_line_dict[LeaderBoardLine.current_driver]
        )
        current_driver_index: int = leaderboard_line_dict[
            LeaderBoardLine.current_driver_index
        ]
        driver_total_times: list[float] = leaderboard_line_dict[
            LeaderBoardLine.driver_total_times
        ]
        missing_mandatory_pitstop: int = leaderboard_line_dict[
            LeaderBoardLine.missing_mandatory_pitstop
        ]
        timing: LeaderboardTiming = LeaderboardTiming.parse_timing(
            leaderboard_line_dict[LeaderBoardLine.timing]
        )
        return LeaderBoardLine(
            car=car,
            current_driver=current_driver,
            current_driver_index=current_driver_index,
            driver_total_times=driver_total_times,
            missing_mandatory_pitstop=missing_mandatory_pitstop,
            timing=timing,
        )


class RaceResult:
    laps = "laps"
    penalties = "penalties"
    post_race_penalties = "post_race_penalties"
    session_index = "sessionIndex"
    race_weekend_index = "raceWeekendIndex"
    session_result = "sessionResult"
    session_type = "sessionType"
    track_name = "trackName"
    server_name = "serverName"
    meta_data = "metaData"
    date = "Date"
    session_file = "SessionFile"
    server_number = "serverNumber"

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
        session_result: dict,  # TODO
        session_type: str,
        track_name: str,
        server_name: str,
        meta_data: str,
        date: datetime,
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
        self.date = date
        self.session_file = session_file
        self.server_number = server_number

    @staticmethod
    def parse_race_result(race_result_dict: dict) -> "RaceResult":
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
            Lap.parse_lap(lap) for lap in race_result_dict[RaceResult.laps]
        ]
        penalties = race_result_dict[RaceResult.penalties]
        post_race_penalties = race_result_dict[RaceResult.post_race_penalties]
        session_index: int = race_result_dict[RaceResult.session_index]
        session_result = race_result_dict[RaceResult.session_result]
        session_type: str = race_result_dict[RaceResult.session_type]
        track_name: str = race_result_dict[RaceResult.track_name]
        server_name: str = race_result_dict[RaceResult.server_name]
        meta_data: str = race_result_dict[RaceResult.meta_data]
        date: datetime = datetime.strptime(
            race_result_dict[RaceResult.date], "%Y-%m-%dT%H:%M:%SZ"
        )
        session_file: str = race_result_dict[RaceResult.session_file]
        server_number: int = race_result_dict[RaceResult.server_number]

        return RaceResult(
            laps=laps,
            penalties=penalties,
            post_race_penalties=post_race_penalties,
            session_index=session_index,
            race_weekend_index=race_result_dict[RaceResult.race_weekend_index],
            session_result=session_result,
            session_type=session_type,
            track_name=track_name,
            server_name=server_name,
            meta_data=meta_data,
            date=date,
            session_file=session_file,
            server_number=server_number,
        )

    def insert_into_database(self, sra_db_cursor: MySQLCursorAbstract) -> None:
        """
        +--------------------+--------------+------+-----+---------+-------+
        | Field              | Type         | Null | Key | Default | Extra |
        +--------------------+--------------+------+-----+---------+-------+
        | uid                | varchar(20)  | NO   | PRI | NULL    |       |
        | session_file       | varchar(20)  | NO   |     | NULL    |       |
        | date               | date         | YES  |     | NULL    |       |
        | track_name         | varchar(255) | YES  |     | NULL    |       |
        | session_type       | varchar(1)   | YES  |     | NULL    |       |
        | session_index      | int(11)      | YES  |     | NULL    |       |
        | race_weekend_index | int(11)      | YES  |     | NULL    |       |
        | server_name        | varchar(255) | YES  |     | NULL    |       |
        | server_number      | int(11)      | YES  |     | NULL    |       |
        | meta_data          | varchar(255) | YES  |     | NULL    |       |
        +--------------------+--------------+------+-----+---------+-------+
        """
        insert_query = f"""
            INSERT INTO races (
                uid, 
                session_file, date, track_name, 
                session_type, session_index, 
                race_weekend_index, 
                server_name, 
                server_number, meta_data
            ) VALUES (
                '{self.session_file}_{self.server_number}', 
                '{self.session_file}', '{self.date}', '{self.track_name}', 
                '{self.session_type}', {self.session_index}, 
                {self.race_weekend_index}, 
                '{Database.handle_bad_string(self.server_name)}', 
                '{self.server_number}', '{self.meta_data}'
            ) ON DUPLICATE KEY UPDATE uid=uid;
        """
        sra_db_cursor.execute(insert_query)


if __name__ == "__main__":
    current_dir = os.path.dirname(__file__)
    ACCSM_dir = os.path.join(current_dir, "ACCSM")
    downloads_dir = os.path.join(ACCSM_dir, "downloads")
    races_dir = os.path.join(downloads_dir, "races")
    # race_result_file = "Zolder_231213_224529_R_server4.json"
    # race_result_json = json.load(open(os.path.join(races_dir, race_result_file)))

    sra_db, sra_db_cursor = Database.connect_database("SRA")
    # parse leaderboard lines
    for i, race_file in enumerate(os.listdir(races_dir)):
        print(f"{i + 1}/{len(os.listdir(races_dir))} - Loading {race_file}")
        server_number = race_file.split("server")[1].split(".")[0]
        with open(os.path.join(races_dir, race_file), "r", encoding="utf-8") as file:
            race_result_json = json.load(file)
        race_result_json["serverNumber"] = server_number

        race_result = RaceResult.parse_race_result(race_result_json)
        race_result.insert_into_database(sra_db_cursor)

        # Commit the transaction
        sra_db.commit()

    pass
