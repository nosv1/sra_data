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