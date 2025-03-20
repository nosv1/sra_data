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