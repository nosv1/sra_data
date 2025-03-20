MATCH (r:Session)
MATCH (q:Session)
WHERE r <> q
    AND r.session_type IN ["R", "R1", "R2"]
    AND q.session_type = "Q"
    AND r.server_name = q.server_name
    AND DATE(r.finish_time) = DATE(q.finish_time)
    AND r.finish_time > q.finish_time
MERGE (q)-[qr:QUALI_TO_RACE]->(r)
// RETURN r, q
// ORDER BY r.session_file DESC