TRUNCATE TABLE tmp.calc_periods;

INSERT INTO tmp.calc_periods
SELECT
    yy.y AS f_y,
    qq.q AS f_q,
    if(qq.q = 0,
        toDate(concat(toString(yy.y), '-01-01')),
        toStartOfQuarter(toDate(concat(toString(yy.y), '-', toString((qq.q - 1) * 3 + 1), '-01')))
    ) AS dt_from,
    if(qq.q = 0,
        toDate(concat(toString(yy.y + 1), '-01-01')),
        addMonths(toStartOfQuarter(toDate(concat(toString(yy.y), '-', toString((qq.q - 1) * 3 + 1), '-01'))), 3)
    ) AS dt_to
FROM (SELECT arrayJoin(range(2019,2028)) AS y) yy
CROSS JOIN (SELECT arrayJoin(range(0,5)) AS q) qq;
