DROP TABLE IF EXISTS tmp.risk_period_source;

CREATE TABLE tmp.risk_period_source
(
    f_y Int32,
    f_q Int32,
    mpioip String,
    risk_id Int8,
    risk_level_id Int8,
    risk_dt Date
)
ENGINE = MergeTree
ORDER BY (f_y, f_q, risk_id, mpioip);
