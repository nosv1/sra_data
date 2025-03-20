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
// WITH tsr, ts

// MATCH (tsr:TeamSeriesRound)-[:TEAM_SERIES_ROUND_TO_TEAM_SERIES_SESSION]->(ts:TeamSeriesSession)
// WITH tsr, COLLECT(ts) AS ts_sessions
// UNWIND ts_sessions AS ts_session
// WITH tsr, ts_session

// ORDER BY tsr.season DESC, tsr.track_name, ts_session.division
// WITH tsr, COLLECT(ts_session.division) AS divisions
// RETURN tsr.season, tsr.track_name, divisions
// ORDER BY tsr.season DESC, tsr.track_name