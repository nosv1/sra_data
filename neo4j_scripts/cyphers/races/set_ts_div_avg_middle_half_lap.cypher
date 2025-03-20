// get all the laps per GT3 Team Series Championship race, sort them, get the middle half laps, average them per race, and set the average to the TeamSeriesSession node
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