DROP TABLE IF EXISTS tmp.calc_periods;

CREATE TABLE tmp.calc_periods
(
    f_y Int32,
    f_q Int32,
    dt_from Date,
    dt_to Date
)
ENGINE = MergeTree
ORDER BY (f_y, f_q);
