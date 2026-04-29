TRUNCATE TABLE tmp.dim_risk_filters;

INSERT INTO tmp.dim_risk_filters
SELECT DISTINCT
    if(bitTest(mask,0),'0',b.f_region_smo_cd),
    if(bitTest(mask,1),'0',b.f_profile_cd),
    if(bitTest(mask,2),'0',b.f_dn_group_cd),
    b.f_y,
    b.f_q,
    if(bitTest(mask,3),'0',b.f_smo),
    if(bitTest(mask,4),'0',b.f_mo)
FROM
(
    SELECT DISTINCT
        if(ifNull(region_smo_cd,'')='','0',region_smo_cd) AS f_region_smo_cd,
        if(ifNull(profile_cd,'')='','0',profile_cd) AS f_profile_cd,
        if(ifNull(group_cd,'')='','0',group_cd) AS f_dn_group_cd,
        f_y,f_q,
        if(ifNull(smo_cd,'')='','0',smo_cd) AS f_smo,
        if(ifNull(mo_cd,'')='','0',mo_cd) AS f_mo
    FROM tmp.risk_precalc
) b
CROSS JOIN (SELECT arrayJoin(range(32)) AS mask);
