// calculate apd for team series qualifyings (apply division offsets)
MATCH (tscc:TeamSeriesCupCategory)
    <-[:TEAM_SERIES_SESSION_TO_CUP_CATEGORY]-(ts:TeamSeriesSession)
    -[:TEAM_SERIES_SESSION_TO_SESSION]->(s:Session)
    <-[:CAR_TO_SESSION]-(c:Car)
WHERE TRUE
    AND c.quali_avg_percent_diff IS NOT NULL
    AND tscc.quali_avg_percent_diff IS NOT NULL
// SET c.ts_quali_avg_percent_diff = c.quali_avg_percent_diff + ts.quali_avg_percent_diff * (ts.season / 13.0) ^ 5
SET c.ts_quali_avg_percent_diff = c.quali_avg_percent_diff + tscc.quali_avg_percent_diff
RETURN s.track_name, ts.season, ts.division, c.car_number, c.pot_best / 1000.0, c.quali_avg_percent_diff, c.ts_quali_avg_percent_diff
ORDER BY ts.season DESC, s.track_name, ts.division ASC, c.pot_best ASC
LIMIT 100