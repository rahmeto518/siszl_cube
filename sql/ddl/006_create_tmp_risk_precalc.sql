DROP TABLE IF EXISTS tmp.risk_precalc;

CREATE TABLE tmp.risk_precalc
(
    f_y Int32,
    f_q Int32,
    mpioip String,
    risk_id Int8,
    risk_level_id Int8,
    risk_dt Date,
    pol_cd Int64,
    age_on_period_end Int32,
    region_smo_cd String,
    profile_cd String,
    group_cd String,
    smo_cd String,
    mo_cd String
)
ENGINE = MergeTree
ORDER BY (f_y, f_q, risk_id, mpioip);
