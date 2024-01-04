/*String sql="SELECT bv, SUM(rate) AS total_rate"+
        "FROM ("+
        "SELECT bv,"+
        " LEAST("+
        " CAST((SELECT COUNT(mid) FROM favorite WHERE bv = d.bv) AS DECIMAL) /"+
        "CAST((SELECT COUNT(mid) FROM view WHERE bv = d.bv) AS DECIMAL),"+
        " 1.0"+
        ") AS rate"+
        "FROM (SELECT DISTINCT bv FROM videos) d"+
        "UNION ALL"+
        "SELECT bv,"+
        "LEAST("+
        "CAST((SELECT COUNT(mid) FROM coin WHERE bv = d.bv) AS DECIMAL) /"+
        "CAST((SELECT COUNT(mid) FROM view WHERE bv = d.bv) AS DECIMAL),"+
        "1.0"+
        ") AS rate"+
        "FROM (SELECT DISTINCT bv FROM videos) d"+
        "UNION ALL"+
        "SELECT bv,"+
        "LEAST("+
        "CAST((SELECT COUNT(mid) FROM public.videolike WHERE bv = d.bv) AS DECIMAL) /"+
        "CAST((SELECT COUNT(mid) FROM view WHERE bv = d.bv) AS DECIMAL),"+
        "1.0"+
        ") AS rate"+
        "FROM (SELECT DISTINCT bv FROM videos) d"+
        "UNION ALL"+
        "SELECT v.bv,"+
        "LEAST("+
        "CASE WHEN COUNT(DISTINCT v2.mid) > 0"+
        "THEN SUM(CAST(v2.viewtime AS DECIMAL)) / (COUNT(DISTINCT v2.mid) * CAST(v.duration AS DECIMAL))"+
        "ELSE 0 END, 1.0"+
        ") AS rate"+
        "FROM videos v"+
        "LEFT JOIN view v2 ON v2.bv = v.bv"+
        "GROUP BY v.bv, v.duration"+
        "UNION ALL"+
        "SELECT bv,"+
        "LEAST("+
        "CAST((SELECT COUNT(mid) FROM danmus WHERE bv = d.bv) AS DECIMAL) / CAST((SELECT COUNT(mid) FROM view WHERE bv = d.bv) AS DECIMAL)," +
        "1.0) AS rate"+
        "FROM (SELECT DISTINCT bv FROM danmus) d"+
        ") AS subquery"+
        "GROUP BY bv order by total_rate desc ;";*/