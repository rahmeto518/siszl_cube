TRUNCATE TABLE tmp.risk_period_source;

INSERT INTO tmp.risk_period_source
SELECT
    cp.f_y,
    cp.f_q,
    r.mpioip,
    r.risk_id,
    argMax(r.risk_level_id, tuple(r.risk_dt, r.risk_level_id)) AS risk_level_id,
    max(r.risk_dt) AS risk_dt
FROM stage.risk r
CROSS JOIN tmp.calc_periods cp
WHERE r.risk_dt >= cp.dt_from
  AND r.risk_dt < cp.dt_to
GROUP BY
    cp.f_y,
    cp.f_q,
    r.mpioip,
    r.risk_id;
