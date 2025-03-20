// assign division number and season number to team series sessions
// "#SRAggTT | Division 2 | Season 13 | GT3 Team Championship | SimRacingAlliance.com | #SRAM1 | cBOP"	
MATCH (s:Session)
WHERE TRUE
    AND s.server_name CONTAINS "GT3 Team Championship"
    // AND s.session_file = "250218_213633_Q"
WITH s,
    toInteger(split(split(s.server_name, "Division ")[1]," ")[0]) as division,
    toInteger(split(split(s.server_name, "Season ")[1]," ")[0]) as season

MERGE (ts:TeamSeriesSession {session_key: s.key_})
ON CREATE SET 
    ts.division = division,
    ts.season = season
with ts, s

MATCH (s)<-[:CAR_TO_SESSION]-(c:Car)
// RETURN ts.season, ts.division, s.track_name, c.car_number, c.cup_category
// ORDER BY ts.season DESC, ts.division ASC, s.track_name ASC, c.cup_category ASC, c.finish_position ASC
with ts, s, COLLECT(DISTINCT c.cup_category) as cup_categories
// RETURN ts.season, ts.division, s.track_name, cup_categories

UNWIND cup_categories as cup_category
MERGE (tscc:TeamSeriesCupCategory {key_: s.key_ + "_" + cup_category})
ON CREATE SET
    tscc.session_key = s.key_,
    tscc.cup_category = cup_category
WITH ts, tscc, s
MERGE (ts)-[:TEAM_SERIES_SESSION_TO_CUP_CATEGORY]->(tscc)
WITH ts, s
MERGE (ts)-[:TEAM_SERIES_SESSION_TO_SESSION]->(s)

// WITH ts, s
// MATCH (tscc:TeamSeriesCupCategory)-[]-(ts)-[]-(s)
// RETURN tscc, ts, s
// LIMIT 10