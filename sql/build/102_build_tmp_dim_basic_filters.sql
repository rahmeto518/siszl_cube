TRUNCATE TABLE tmp.dim_basic_filters;

INSERT INTO tmp.dim_basic_filters
SELECT DISTINCT
    if(ifNull(smo_cd, '') = '', '0', substring(smo_cd, 1, 2)) AS f_region_smo_cd,
    toString(ifNull(profile_cd, 0)) AS f_profile_cd,
    toString(ifNull(group_cd, 0)) AS f_dn_group_cd,
    toYear(in_dt) AS f_y,
    0 AS f_q,
    if(ifNull(smo_cd, '') = '', '0', smo_cd) AS f_smo,
    if(ifNull(attach_mo_cd, '') = '', '0', attach_mo_cd) AS f_mo
FROM tmp.for_basic_metrics;
