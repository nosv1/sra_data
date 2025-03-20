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