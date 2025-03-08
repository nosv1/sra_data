from neo4j import Session

from utils.Database import Neo4jDatabase


def process_sra_db_neo(neo_session: Session):
    # create quali to race relationships
    quali_to_race_query = """
        MATCH (r:Session)
        MATCH (q:Session)
        WHERE r <> q
            AND r.session_type = "R"
            AND q.session_type = "Q"
            AND r.server_name = q.server_name
            AND DATE(r.finish_time) = DATE(q.finish_time)
            AND r.finish_time > q.finish_time
        MERGE (q)-[qr:QUALI_TO_RACE]->(r)
        // RETURN r, q
        // ORDER BY r.session_file DESC
    """
    print("Creating quali to race relationships...", end="")
    neo_session.run(quali_to_race_query)
    print("done")

    # set div num for ts sessions
    team_series_query = """
        // assign division number and season number to team series sessions
        // "#SRAggTT | Division 2 | Season 13 | GT3 Team Championship | SimRacingAlliance.com | #SRAM1 | cBOP"	
        MATCH (s:Session)
        WHERE TRUE
            AND s.server_name CONTAINS "GT3 Team Championship"
        WITH s,
            toInteger(split(split(s.server_name, "Division ")[1]," ")[0]) as division,
            toInteger(split(split(s.server_name, "Season ")[1]," ")[0]) as season

        MERGE (ts:TeamSeriesSession {session_key: s.key_})
        ON CREATE SET 
            ts.division = division,
            ts.season = season
        MERGE (ts)-[:TEAM_SERIES_SESSION_TO_SESSION]->(s)
    """
    print("Assigning division and season numbers to team series sessions...", end="")
    neo_session.run(team_series_query)
    print("done")

    # create team series weekend nodes
    team_series_weekend_query = """
        // links team series sessions for when the server names, tracks, divisions, and seasons are the same
        MATCH (ts:TeamSeriesSession)-[:TEAM_SERIES_SESSION_TO_SESSION]->(s:Session)
        WITH ts, s

        MATCH (ts2:TeamSeriesSession)-[:TEAM_SERIES_SESSION_TO_SESSION]->(s2:Session)
        WHERE ts <> ts2
            AND s.server_name = s2.server_name
            AND s.track_name = s2.track_name
            AND ts.season = ts2.season
            AND ts.division = ts2.division
        MERGE (tsw:TeamSeriesWeekend {server_name: s.server_name, track_name: s.track_name, season: ts.season, division: ts.division, key_: "" + ts.season + "_" + ts.division + "_" + s.track_name})
        MERGE (tsw)-[:TEAM_SERIES_WEEKEND_TO_TEAM_SERIES_SESSION]->(ts)
        MERGE (tsw)-[:TEAM_SERIES_WEEKEND_TO_TEAM_SERIES_SESSION]->(ts2)
        // RETURN tsw, ts, ts2
    """
    print("Creating team series weekend nodes...", end="")
    neo_session.run(team_series_weekend_query)
    print("done")

    # create team series round nodes
    team_series_rounds_query = """
        // create a node for team series rounds that connects all the team series sessions that match season and track name
        MATCH (ts:TeamSeriesSession)-[:TEAM_SERIES_SESSION_TO_SESSION]->(s:Session)
        WITH ts, s
        MATCH (ts2:TeamSeriesSession)-[:TEAM_SERIES_SESSION_TO_SESSION]->(s2:Session)
        WHERE TRUE 
            AND ts <> ts2 
            AND ts.season = ts2.season 
            AND s.track_name = s2.track_name
        WITH ts, s, COLLECT(ts2) as other_sessions

        MERGE (tsr:TeamSeriesRound {season: ts.season, track_name: s.track_name, key_:""+ts.season+"_"+s.track_name})
        MERGE (tsr)-[:TEAM_SERIES_ROUND_TO_TEAM_SERIES_SESSION]->(ts)
        FOREACH (ts2 IN other_sessions | MERGE (tsr)-[:TEAM_SERIES_ROUND_TO_TEAM_SERIES_SESSION]->(ts2))
        WITH tsr, ts

        MATCH (tsr:TeamSeriesRound)-[:TEAM_SERIES_ROUND_TO_TEAM_SERIES_SESSION]->(ts:TeamSeriesSession)
        WITH tsr, COLLECT(ts) AS ts_sessions
        UNWIND ts_sessions AS ts_session
        WITH tsr, ts_session

        ORDER BY tsr.season DESC, tsr.track_name, ts_session.division
        WITH tsr, COLLECT(ts_session.division) AS divisions
        RETURN tsr.season, tsr.track_name, divisions
        ORDER BY tsr.season DESC, tsr.track_name
    """
    print("Creating team series round nodes...", end="")
    neo_session.run(team_series_rounds_query)
    print("done")

    # create team series season nodes
    team_series_season_query = """
        // create a node for each team series season that stores the max number of divisions and connects all the team series sessions in that season to it
        MATCH (ts:TeamSeriesSession)
        WITH ts.season as season, ts.division as division
        ORDER BY season, division
        WITH season, COLLECT(DISTINCT division) as divisions

        MERGE (tss:TeamSeriesSeason {season: season})
        SET tss.max_divisions = divisions[-1]
        WITH tss, season

        MATCH (ts:TeamSeriesSession)
        WHERE ts.season = season
        MERGE (tss)-[:TEAM_SERIES_SEASON_TO_TEAM_SERIES_SESSION]->(ts)
    """
    print("Creating team series season nodes...", end="")
    neo_session.run(team_series_season_query)
    print("done")

    # set average middle half lap per div per team series race
    team_series_avg_middle_half_lap_query = """
        // get all the laps per GT3 Team Series Championship race, sort them, get the middle half laps, average them per race, create a node and connect it to the race session
        MATCH (ts:TeamSeriesSession)-[:TEAM_SERIES_SESSION_TO_SESSION]->(s:Session)
        MATCH (s)<-[:LAP_TO_SESSION]-(l:Lap)
        with s, ts, l
        ORDER BY l.lap_time ASC
        WHERE TRUE 
            AND l.lap_number > 1
            AND l.is_valid_for_best = TRUE
            AND s.session_type = "R"
        WITH s, ts, COLLECT(l) AS laps
        WITH s, ts, laps,
            laps[SIZE(laps)/4] AS p25_lap, 
            laps[SIZE(laps)*3/4] AS p75_lap
        WITH s, ts, p25_lap, p75_lap, 
            [lap IN laps WHERE lap.lap_time >= p25_lap.lap_time AND lap.lap_time <= p75_lap.lap_time] AS middle_50_laps
        WITH s, ts, REDUCE(sum = 0, lap IN middle_50_laps | sum + lap.lap_time) / SIZE(middle_50_laps) AS avg_middle_half_lap

        SET ts.avg_middle_half_lap = avg_middle_half_lap
        // RETURN s.track_name, ts.season, ts.division, avg_middle_half_lap / 1000.0
        // ORDER BY ts.season DESC, s.track_name, ts.division ASC
    """
    print("Calculating average field lap per team series race...", end="")
    neo_session.run(team_series_avg_middle_half_lap_query)
    print("done")

    # set avg_percent_diff for ts divs
    team_series_avg_percent_diff_query = """
        // calculate average percent difference against each division for each track 
        // average percent difference should only be calculated if every division has the same conditions (is_wet_session should be 0 for all divisions per track)
        // average percent difference = sum(div / other_div[i]) / count(other_div) ----- don't include the division itself

        // get min and max season number, unwind the season range, collect distinct track season combos
        MATCH (ts:TeamSeriesSession)
        WITH min(ts.season) as min_season, max(ts.season) as max_season
        UNWIND range(min_season, max_season) as season
        MATCH (ts:TeamSeriesSession {season: season})-[:TEAM_SERIES_SESSION_TO_SESSION]->(s:Session)
        WITH DISTINCT s.track_name as track_name, ts.season as season, COLLECT(s.is_wet_session) as wet_sessions
        ORDER BY season, track_name
        WITH season, track_name, COLLECT(DISTINCT {track_name: track_name, season: season, wet_sessions: wet_sessions}) as track_season_combos
        // RETURN track_season_combos
        // ORDER BY season desc, track_name

        // for each track season combo, get the number of wet sessions and the number of divisions
        UNWIND track_season_combos as combo
        WITH combo.track_name as track_name, combo.season as season
        MATCH (tss:TeamSeriesSeason)-[]->(ts:TeamSeriesSession {season: season})-[:TEAM_SERIES_SESSION_TO_SESSION]->(s:Session {track_name: track_name})
        WHERE s.session_type = "R"
        WITH tss, track_name, season, COLLECT(DISTINCT ts.division) as divisions, SUM(s.is_wet_session) as num_wet_sessions
        WHERE TRUE
            AND num_wet_sessions = 0
            AND SIZE(divisions) = tss.max_divisions
        WITH track_name, season, divisions, num_wet_sessions
        // RETURN track_name, season, divisions, num_wet_sessions
        // ORDER BY season desc, track_name

        // for each track season combo, get the average percentile lap time for each division
        UNWIND divisions as division
        MATCH (ts1:TeamSeriesSession {season: season, division: division})-[:TEAM_SERIES_SESSION_TO_SESSION]->(s:Session {track_name: track_name})
        WHERE s.session_type = "R"
        WITH track_name, season, divisions, division, ts1
            UNWIND divisions as other_div
            MATCH (ts2:TeamSeriesSession {season: season, division: other_div})-[:TEAM_SERIES_SESSION_TO_SESSION]->(s:Session {track_name: track_name})
            WHERE s.session_type = "R"
            WITH track_name, season, division, ts1, other_div, ts2
            WHERE division <> other_div
        WITH track_name, season, division, ts1, COLLECT(ts1.avg_middle_half_lap / toFloat(ts2.avg_middle_half_lap) - 1) as percent_diffs
        // RETURN track_name, season, division, ts1.avg_middle_half_lap, percent_diffs
        // ORDER BY season desc, track_name, division

        // for each track season combo, calculate the average percent difference for each division
        WITH track_name, season, division, ts1, REDUCE(s = 0.0, x IN percent_diffs | s + x) / SIZE(percent_diffs) as avg_percent_diff
        SET ts1.avg_percent_diff = avg_percent_diff
        // RETURN track_name, season, division, avg_percent_diff
        // ORDER BY season desc, track_name, division
    """
    print(
        "Calculating average percent difference for team series divisions...",
        end="",
    )
    neo_session.run(team_series_avg_percent_diff_query)
    print("done")

    # set average middle half lap per car per race
    car_avg_middle_half_lap_query = """
        // get all the laps per car per race, sort them, get the middle half laps, average them, and store the average in the car node
        MATCH (s:Session)<-[:LAP_TO_SESSION]-(l:Lap)-[:LAP_TO_CAR]->(c:Car)<-[:DRIVER_TO_CAR]-(d:Driver)
        WITH s, l, c, d
        ORDER BY l.lap_time ASC
        WHERE TRUE 
            AND l.lap_number > 1
            AND s.session_type STARTS WITH "R"
        WITH s, c, d, COLLECT(l) AS laps, [lap IN COLLECT(l) WHERE lap.is_valid_for_best = TRUE] AS valid_laps
        WHERE TRUE
            AND SIZE(valid_laps) > 5
        // RETURN s.session_file, s.track_name
        // ORDER BY s.session_file DESC
        // LIMIT 100

        WITH s, c, d, laps, valid_laps,
            valid_laps[SIZE(valid_laps)/4] AS p25_lap, 
            valid_laps[SIZE(valid_laps)*3/4] AS p75_lap
        WITH s, c, d, laps, p25_lap, p75_lap, 
            [vl IN valid_laps WHERE vl.lap_time >= p25_lap.lap_time AND vl.lap_time <= p75_lap.lap_time] AS middle_50_laps

        // cars must have finished at least 75% of the race, that's 10 laps for a 20 minute race on a 90 second track, so 5 laps as middle 50%
        MATCH (winner:Car)-[:CAR_TO_SESSION]->(s)
        WHERE TRUE 
            AND winner.finish_position = 1
        WITH s, c, d, laps, middle_50_laps, winner
        WHERE TRUE
            AND (SIZE(laps) + 1) >= 0.75 * winner.lap_count
        // RETURN s.session_file, s.server_number, d.first_name, d.last_name
        // RETURN s.session_file, s.track_name, SIZE(laps), winner.lap_count, winner.lap_count * 0.75
        // ORDER BY s.session_file DESC
        // LIMIT 100
        WITH s, c, d, REDUCE(sum = 0, lap IN middle_50_laps | sum + lap.lap_time) / SIZE(middle_50_laps) AS avg_middle_half_lap

        SET c.avg_middle_half_lap = avg_middle_half_lap
        // RETURN s.session_file, s.track_name, d.division, d.first_name + " " + d.last_name, c.car_number, avg_middle_half_lap / 1000.0
        // ORDER BY s.session_file DESC, s.track_name ASC, c.avg_middle_half_lap ASC
        // LIMIT 100
    """
    print(
        "Calculating average field lap per car per race...",
        end="",
    )
    neo_session.run(car_avg_middle_half_lap_query)
    print("done")

    # set avg_percent_diff for cars
    car_avg_percent_diff_query = """
        // calculate the average percent difference for each car.average_middle_half_lap in each race
        // average percent difference = sum(car.avg_middle_half_lap / other_car[i].avg_middle_half_lap) / count(other_car)
        MATCH (s:Session)<-[:CAR_TO_SESSION]-(c:Car)
        WITH s, [c IN COLLECT(c) WHERE c.avg_middle_half_lap IS NOT NULL] as cars
        WHERE TRUE 
            AND SIZE(cars) > 1
        UNWIND cars as c
        WITH s, c, 
            REDUCE(s = 0.0, oc IN cars | s + CASE WHEN c <> oc THEN (c.avg_middle_half_lap / toFloat(oc.avg_middle_half_lap)) - 1 ELSE 0 END) / (SIZE(cars) - 1) as avg_percent_diff
        SET c.avg_percent_diff = avg_percent_diff
        // RETURN s.track_name, s.session_file, s.server_number, c.car_number, c.avg_middle_half_lap / 1000.0, c.avg_percent_diff
        // ORDER BY s.session_file DESC, s.track_name ASC, c.avg_middle_half_lap ASC
        // LIMIT 100
    """
    print(
        "Calculating average percent difference for cars...",
        end="",
    )
    neo_session.run(car_avg_percent_diff_query)
    print("done")

    # set team series percent diffs
    team_series_avg_percent_diff_query = """
        // calculate apd for team series races (apply division offsets)
        MATCH (ts:TeamSeriesSession)-[:TEAM_SERIES_SESSION_TO_SESSION]->(s:Session)<-[:CAR_TO_SESSION]-(c:Car)
        WHERE TRUE
            AND c.avg_percent_diff IS NOT NULL
            AND ts.avg_percent_diff IS NOT NULL
        // SET c.ts_avg_percent_diff = c.avg_percent_diff + ts.avg_percent_diff * (ts.season / 13.0) ^ 5
        SET c.ts_avg_percent_diff = c.avg_percent_diff + ts.avg_percent_diff
        // RETURN s.track_name, ts.season, ts.division, c.car_number, c.avg_middle_half_lap / 1000.0, c.avg_percent_diff, c.ts_avg_percent_diff
        // ORDER BY ts.season DESC, s.track_name, ts.division ASC, c.avg_middle_half_lap ASC
        // LIMIT 100
    """
    print(
        "Calculating team series average percent difference...",
        end="",
    )
    neo_session.run(team_series_avg_percent_diff_query)
    print("done")


if __name__ == "__main__":
    neo_driver, neo_session = Neo4jDatabase.connect_database("SRA")
    process_sra_db_neo(neo_session)
    Neo4jDatabase.close_connection(neo_driver, neo_session)
