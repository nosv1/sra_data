// get all the laps per car per race, sort them, get the middle half laps, average them, and store the average in the car node
MATCH (s:Session)<-[:LAP_TO_SESSION]-(l:Lap)-[:LAP_TO_CAR]->(c:Car)<-[:DRIVER_TO_CAR]-(d:Driver)
WITH s, l, c, d
ORDER BY l.lap_time ASC
WHERE TRUE 
    AND l.lap_number > 1
    AND s.session_type STARTS WITH "R"
    AND NOT c.best_split1 IN [2147483647, 9223372036854775807]
WITH s, c, d, l, c.best_split1 + c.best_split2 + c.best_split3 AS pot_best
SET c.pot_best = pot_best
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