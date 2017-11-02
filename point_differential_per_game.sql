SELECT *, round(cum_diff / gp::decimal, 1) AS cum_diff_pg
FROM (
    SELECT distinct d.team, d.date, w.cum_diff, g.gp
    FROM (SELECT DISTINCT ON (team) team, date
        FROM box_score
        ORDER BY team, date DESC) d
    INNER JOIN (SELECT team, date, SUM(diff) OVER (PARTITION BY team ORDER BY date) AS cum_diff
        FROM box_score) w ON w.date = d.date
    AND w.team = d.team
    INNER JOIN (SELECT team, COUNT(*) AS gp
                FROM box_score
                GROUP BY team) g ON d.team = g.team
    ) dpg
ORDER BY cum_diff_pg desc, gp desc, date desc;
