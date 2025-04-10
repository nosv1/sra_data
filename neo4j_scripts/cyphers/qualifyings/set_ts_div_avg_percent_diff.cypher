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
WHERE s.session_type = "Q"
WITH tss, track_name, season, COLLECT(DISTINCT ts.division) as divisions, SUM(s.is_wet_session) as num_wet_sessions
WHERE TRUE
    AND num_wet_sessions = 0
    AND SIZE(divisions) = tss.max_divisions
WITH track_name, season, divisions, num_wet_sessions
// RETURN track_name, season, divisions, num_wet_sessions
// ORDER BY season desc, track_name

// for each track season combo, get the average percentile potential best time for each division
UNWIND divisions as division
MATCH (tscc1:TeamSeriesCupCategory)
    <-[:TEAM_SERIES_SESSION_TO_CUP_CATEGORY]-(ts1:TeamSeriesSession {season: season, division: division})
    -[:TEAM_SERIES_SESSION_TO_SESSION]->(s:Session {track_name: track_name})
WHERE TRUE
    AND s.session_type = "Q"
    AND tscc1.avg_middle_half_pot_best IS NOT NULL
WITH track_name, season, divisions, division, tscc1, ts1
    UNWIND divisions as other_div
    MATCH (tscc2:TeamSeriesCupCategory)
    <-[:TEAM_SERIES_SESSION_TO_CUP_CATEGORY]-(ts2:TeamSeriesSession {season: season, division: other_div})
    -[:TEAM_SERIES_SESSION_TO_SESSION]->(s:Session {track_name: track_name})
    WHERE TRUE 
        AND s.session_type = "Q" 
        AND tscc2.avg_middle_half_pot_best IS NOT NULL
    WITH track_name, season, division, tscc1, ts1, other_div, tscc2, ts2
    ORDER BY other_div, tscc2.cup_category
    WHERE NOT (division = other_div AND tscc1.cup_category = tscc2.cup_category)
    WITH track_name, season, division, tscc1, ts1, 
        // COLLECT(division + "" + tscc1.cup_category + "/" + other_div + "" + tscc2.cup_category) as ccs,
        COLLECT(tscc1.avg_middle_half_pot_best / toFloat(tscc2.avg_middle_half_pot_best) - 1) as percent_diffs
// RETURN track_name, season, division, tscc1.cup_category, tscc1.avg_middle_half_pot_best, ccs, percent_diffs
// ORDER BY season desc, track_name, division, tscc1.cup_category
// LIMIT 100

// for each track season combo, calculate the average percent difference for each division
WHERE SIZE(percent_diffs) > 0
WITH track_name, season, division, tscc1, ts1, REDUCE(s = 0.0, x IN percent_diffs | s + x) / SIZE(percent_diffs) as quali_avg_percent_diff
SET tscc1.quali_avg_percent_diff = quali_avg_percent_diff
// RETURN track_name, season, division, tscc1.cup_category, quali_avg_percent_diff
// ORDER BY season desc, track_name, division, tscc1.cup_category
// LIMIT 100