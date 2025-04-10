// calculate the average percent difference for each car.pot_best in each qualifying
// average percent difference = sum(car.pot_best / other_car[i].pot_best) / count(other_car)
MATCH (s:Session)<-[:CAR_TO_SESSION]-(c:Car)
WHERE TRUE 
    AND s.session_type = "Q" 
    AND NOT c.best_split1 IN [2147483647, 9223372036854775807]
WITH s, c, c.best_split1 + c.best_split2 + c.best_split3 AS pot_best
SET c.pot_best = pot_best
WITH s, COLLECT(c) as cars
WHERE SIZE(cars) > 1
// RETURN s, cars
// LIMIT 1
UNWIND cars as c
WITH s, c, 
    REDUCE(s = 0.0, oc IN cars | s + CASE WHEN c <> oc THEN (c.pot_best / toFloat(oc.pot_best)) - 1 ELSE 0 END) / (SIZE(cars) - 1) as quali_avg_percent_diff
SET c.quali_avg_percent_diff = quali_avg_percent_diff
// RETURN s.track_name, s.session_file, s.server_number, c.car_number, c.pot_best / 1000.0, c.quali_avg_percent_diff
// ORDER BY s.session_file DESC, s.track_name ASC, c.pot_best ASC
// LIMIT 100