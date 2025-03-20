// calculate the average percent difference for each car.average_middle_half_lap in each race
// average percent difference = sum(car.avg_middle_half_lap / other_car[i].avg_middle_half_lap) / count(other_car)
MATCH (s:Session)<-[:CAR_TO_SESSION]-(c:Car)
WITH s, [c IN COLLECT(c) WHERE c.avg_middle_half_lap IS NOT NULL] as cars
WHERE TRUE 
    AND SIZE(cars) > 1
    AND s.session_type STARTS WITH "R"
UNWIND cars as c
WITH s, c, 
    REDUCE(s = 0.0, oc IN cars | s + CASE WHEN c <> oc THEN (c.avg_middle_half_lap / toFloat(oc.avg_middle_half_lap)) - 1 ELSE 0 END) / (SIZE(cars) - 1) as avg_percent_diff
SET c.avg_percent_diff = avg_percent_diff
// RETURN s.track_name, s.session_file, s.server_number, c.car_number, c.avg_middle_half_lap / 1000.0, c.avg_percent_diff
// ORDER BY s.session_file DESC, s.track_name ASC, c.avg_middle_half_lap ASC
// LIMIT 100