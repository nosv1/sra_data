// MATCH (d:Driver)<-[db:DRIVEN_BY]-(l:Lap)-[sd:SET_DURING]->(s:Session)<-[ui:USED_IN]-(c:Car)
// RETURN d, db, l, sd, s, ui, c
// LIMIT 100

// MATCH (s:Session)<-[di:DROVE_IN]-(d:Driver)
// WHERE 
//     s.server_name CONTAINS "GT3 Team Championship"
//     AND s.session_type = "Q"
//     AND s.server_name CONTAINS "Season 12"
//     AND s.server_name CONTAINS "Division 5"
// RETURN s, di, d
// LIMIT 1000

/*
This query finds the minimum lap time for each driver in the specified sessions.
It filters sessions based on server name and session type, then returns the server name,
track name, driver name, and minimum lap time, ordered by session file and lap time.
*/
// MATCH (d:Driver)<-[db:DRIVEN_BY]-(l:Lap)-[sd:SET_DURING]->(s:Session)
// WHERE 
//     s.server_name CONTAINS "GT3 Team Championship"
//     AND s.session_type = "Q"
//     AND s.server_name CONTAINS "Season 12"
//     AND s.server_name CONTAINS "Division 5"
// WITH d, s, MIN(l.lap_time) AS minLapTime
// RETURN s.server_name, s.track_name, d.first_name + " " + d.last_name as driver_name, minLapTime
// ORDER BY s.session_file DESC, minLapTime ASC
// LIMIT 1000

/*
This query calculates the median lap time for each division in the specified sessions.
It filters sessions based on server name and session type, then returns the server name,
track name, division, median lap time (in seconds), and whether the session was wet,
ordered by track name and division.
*/
UNWIND range(1, 6) AS division
UNWIND range(11, 11) as season
MATCH (d:Driver)<-[db:DRIVEN_BY]-(l:Lap)-[sd:SET_DURING]->(s:Session)
WHERE 
    s.server_name CONTAINS "GT3 Team Championship"
    AND s.session_type = "R"
    AND s.server_name CONTAINS "Season " + season
    AND s.server_name CONTAINS "Division " + division
    AND s.is_wet_session = 0
    // we don't check for valid laps in races, assuming the percentiles will avoid outliers
    // AND l.is_valid_for_best
WITH s, l, division, season
ORDER BY l.lap_time
WITH s, collect(l.lap_time) AS lapTimes, division, season
WITH s, lapTimes, size(lapTimes) AS count, division, season
WITH s, lapTimes, count, division, season,
    lapTimes[0] / 1000.0 AS firstPercentile,
    lapTimes[(count / 4) - 1] / 1000.0  AS twentyFifthPercentile,
    lapTimes[(count / 2) - 1] / 1000.0  AS fiftiethPercentile,
    lapTimes[(3 * (count / 4)) - 1] / 1000.0  AS seventyFifthPercentile
WITH s, division, season, firstPercentile, twentyFifthPercentile, fiftiethPercentile, seventyFifthPercentile,
    // we don't include firstPercentile in the average because we're not checking for valid laps in races
    round((twentyFifthPercentile + fiftiethPercentile + seventyFifthPercentile) / 3, 3) AS averagePercentile
RETURN season, s.track_name as track, division as div, averagePercentile, firstPercentile, twentyFifthPercentile, fiftiethPercentile, seventyFifthPercentile
ORDER BY season DESC, s.week_of_year ASC, division ASC
LIMIT 1000

UNWIND range(12, 12) as season
UNWIND range(1, 6) AS division
MATCH (d:Driver)<-[db:DRIVEN_BY]-(l:Lap)-[sd:SET_DURING]->(s:Session)
MATCH (d)-[drv:DROVE]->(c:Car)-[ui:USED_IN]->(s)
WHERE 
    s.server_name CONTAINS "GT3 Team Championship"
    AND s.session_type = "R"
    AND s.server_name CONTAINS "Season " + season
    AND s.server_name CONTAINS "Division " + division
WITH s, c, d, l, division
ORDER BY l.lap_time
WITH s, c, d, division, collect(l.lap_time) AS lapTimes, sum(l.lap_time) AS totalTime, [lap IN collect(l) WHERE lap.is_valid_for_best | lap.lap_time] AS validLapTimes
WITH s, c, d, division, validLapTimes, lapTimes, size(validLapTimes) AS count, totalTime
WHERE
    count >= 5
WITH s, c, d, division, validLapTimes, lapTimes, count, toFloat(size(validLapTimes)) / toFloat(size(lapTimes)) AS validLapPercentage, totalTime,
    validLapTimes[0] / 1000.0 AS firstPercentile,
    validLapTimes[(count / 4) - 1] / 1000.0  AS twentyFifthPercentile,
    validLapTimes[(count / 2) - 1] / 1000.0  AS fiftiethPercentile,
    validLapTimes[(3 * (count / 4)) - 1] / 1000.0  AS seventyFifthPercentile
WITH s, c, d, division, validLapTimes, lapTimes, count, validLapPercentage, totalTime, firstPercentile, twentyFifthPercentile, fiftiethPercentile, seventyFifthPercentile,
    round((firstPercentile + twentyFifthPercentile + fiftiethPercentile + seventyFifthPercentile) / 4, 3) AS averagePercentile
WITH s, c, d, division, validLapPercentage, totalTime, firstPercentile, twentyFifthPercentile, fiftiethPercentile, seventyFifthPercentile, averagePercentile,
    round(abs(averagePercentile - fiftiethPercentile), 3) as consistency
ORDER BY s.week_of_year ASC, division ASC, averagePercentile ASC
RETURN s.track_name, division, d.driver_id, d.first_name, d.last_name, c.finish_position, averagePercentile, consistency, validLapPercentage, totalTime, firstPercentile, twentyFifthPercentile, fiftiethPercentile, seventyFifthPercentile
LIMIT 100
MERGE (drp:DriverRaceProcessed {
    driver_id: d.driver_id,
    session_key: s.key_,
    car_key: c.key_,
    key_: s.session_file + "_" + s.server_number + "_" + d.driver_id
})
SET drp.first_percentile = firstPercentile,
    drp.twenty_fifth_percentile = twentyFifthPercentile,
    drp.fiftieth_percentile = fiftiethPercentile,
    drp.seventy_fifth_percentile = seventyFifthPercentile,
    drp.validLapPercentage = validLapPercentage
    drp.average_percentile = averagePercentile
    drp.consistency = consistency
    drp.total_time = totalTime
MERGE (drp)-[:DRIVER_RACE_PROCESSED]->(d)
RETURN drp
LIMIT 1000

MATCH (d:Driver)<-[db:DRIVEN_BY]-(l:Lap)-[sd:SET_DURING]->(s:Session)
WHERE 
    d.last_name = "Arc"
    AND s.track_name = "paul_ricard"
    AND s.server_name CONTAINS "Division 2"
RETURN l.lap_time, l.is_valid_for_best


MATCH (d:Driver)<-[db:DRIVEN_BY]-(l:Lap)-[sd:SET_DURING]->(s:Session)
WHERE 
    d.first_name = "Michael"
    AND d.last_name = "Jeffries"
    AND s.server_name CONTAINS "GT3 Team Championship"
WITH substring(s.server_name, indexOf(s.server_name, "Season "), 8) AS season
RETURN distinct season

// MATCH (d:Driver)<-[db:DRIVEN_BY]-(l:Lap)-[sd:SET_DURING]->(s:Session)
MATCH (d:Driver)-[db:DROVE]->(c:Car)-[ui:USED_IN]->(s:Session)
WHERE 
    // d.first_name = "Michael"
    // AND d.last_name = "Jeffries"
    d.driver_id = "S76561197977811312"
    AND s.server_name CONTAINS "GT3 Team Championship"
    AND s.server_name CONTAINS "Season "
    AND s.session_type = "R"
WITH s, c, split(s.server_name, ' ') AS list
WITH s, c, list, range(0, size(list) - 1) AS indexes
WITH s, c, list, reduce(acc=-1, index IN indexes | 
    CASE WHEN list[index] = 'Season' THEN index ELSE acc END
) AS seasonIndex
WITH s, c, seasonIndex, toInteger(list[seasonIndex + 1]) AS season
RETURN s.session_file, s.server_name, c.finish_position, season
ORDER BY s.session_file


MATCH (d:Driver)<-[db:DRIVEN_BY]-(l:Lap)-[sd:SET_DURING]->(s:Session)
WHERE s.server_name CONTAINS "GT3 Team Championship"
WITH d, s, toInteger(split(split(s.server_name, "Division ")[1]," ")[0]) AS currentDivision
// return d.first_name, d.last_name, currentDivision
// limit 10
ORDER BY d.driver_id, s.finish_time
WITH d, collect(currentDivision) AS divisions
WITH d, [i IN range(1, size(divisions) - 1) WHERE divisions[i] > divisions[i - 1]] AS higherDivisions
WHERE size(higherDivisions) > 0
RETURN d.driver_id, d.first_name, d.last_name, higherDivisions
LIMIT 100