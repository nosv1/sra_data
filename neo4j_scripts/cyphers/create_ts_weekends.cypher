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