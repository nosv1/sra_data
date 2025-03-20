// get all the pot bests per GT3 Team Series Championship qualifying, sort them, get the middle half laps, average them per qualifying, and set the average to the TeamSeriesSession node
// also set the car's potential best time to the sum of its best splits
MATCH (_:TeamSeriesCupCategory)
WITH COLLECT(DISTINCT _.cup_category) AS cup_categories
// RETURN cup_categories

UNWIND cup_categories AS cup_category
MATCH (ts:TeamSeriesSession)
    -[:TEAM_SERIES_SESSION_TO_SESSION]->(s:Session)
    <-[:CAR_TO_SESSION]-(c:Car)
WHERE TRUE 
    AND c.best_split1 <> 2147483647 
    AND s.session_type = "Q" 
    AND c.cup_category = cup_category 
    // AND s.session_file = "250218_213633_Q"
WITH cup_category, s, ts, c
     ORDER BY c.pot_best ASC
// RETURN s.track_name, tscc.cup_category, c.car_number, c.pot_best
// ORDER BY ts.season DESC, ts.division ASC, s.track_name ASC, tscc.cup_category ASC, c.finish_position ASC
WITH cup_category, s, ts, COLLECT(c.pot_best) AS pot_bests
// RETURN s.track_name, ts.season, ts.division, cup_category, pot_bests
WITH cup_category, s, ts, pot_bests, pot_bests[SIZE(pot_bests) / 4] AS p25_lap,
     pot_bests[SIZE(pot_bests) * 3 / 4] AS p75_lap
WITH cup_category, s, ts, p25_lap, p75_lap,
     [pot_best IN pot_bests WHERE pot_best >= p25_lap AND pot_best <= p75_lap]
     AS middle_50_pot_bests
// RETURN p25_lap, p75_lap
WITH cup_category, s,
     ts, REDUCE(sum = 0, pot_best IN middle_50_pot_bests | sum + pot_best) /
         SIZE(middle_50_pot_bests) AS avg_middle_half_pot_best
// RETURN s.track_name, ts.season, ts.division, cup_category, avg_middle_half_pot_best / 1000.0
// ORDER BY ts.season DESC, s.track_name, ts.division ASC, tscc.cup_category ASC

MATCH (tscc:TeamSeriesCupCategory {cup_category: cup_category})
    <-[:TEAM_SERIES_SESSION_TO_CUP_CATEGORY]-(ts)
    -[:TEAM_SERIES_SESSION_TO_SESSION]->(s)
WHERE TRUE
    AND tscc.cup_category = cup_category
    AND s.session_type = "Q"
SET tscc.avg_middle_half_pot_best = avg_middle_half_pot_best
WITH ts, s, tscc, cup_category
RETURN s.track_name, ts.season, ts.division, cup_category, tscc.avg_middle_half_pot_best / 1000.0
ORDER BY ts.season DESC, s.track_name, ts.division ASC, cup_category ASC
LIMIT 100