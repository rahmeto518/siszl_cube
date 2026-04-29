TRUNCATE TABLE tmp.risk_precalc;

INSERT INTO tmp.risk_precalc
SELECT
    r.f_y,
    r.f_q,
    r.mpioip,
    r.risk_id,
    r.risk_level_id,
    r.risk_dt,
    p.pol_cd,
    dateDiff('year', p.birth_dt,
        if(r.f_q = 0,
            toDate(concat(toString(r.f_y), '-12-31')),
            addDays(addMonths(toStartOfQuarter(toDate(concat(toString(r.f_y), '-', toString((r.f_q - 1)*3 + 1), '-01'))),3),-1)
        )
    ) AS age_on_period_end,
    if(ifNull(p.smo_cd,'')='','0',substring(p.smo_cd,1,2)) AS region_smo_cd,
    toString(any(profile_cd)) AS profile_cd,
    toString(any(group_cd)) AS group_cd,
    p.smo_cd,
    p.attach_mo_cd AS mo_cd
FROM tmp.risk_period_source r
LEFT JOIN stage.dispans_person p ON p.mpioip = r.mpioip
LEFT JOIN tmp.for_basic_metrics bm ON bm.mpioip = r.mpioip
GROUP BY
    r.f_y, r.f_q, r.mpioip, r.risk_id, r.risk_level_id, r.risk_dt,
    p.pol_cd, p.birth_dt, p.smo_cd, p.attach_mo_cd;
