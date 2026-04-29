TRUNCATE TABLE cube.basic_metrics;

INSERT INTO cube.basic_metrics
SELECT
    f.f_region_smo_cd,
    f.f_profile_cd,
    f.f_dn_group_cd,
    f.f_y,
    f.f_q,
    f.f_smo,
    f.f_mo,
    uniqExact(x.disp_id) AS sl_dn_cnt,
    uniqExactIf(x.mpioip, x.out_dt IS NULL) AS zl_on_dn_total,
    uniqExactIf(x.mpioip, x.death_dt IS NOT NULL) AS death_new,
    uniqExactIf(x.task_id, x.task_id IS NOT NULL AND x.task_status_cd != 5) AS all_tasks,
    uniqExactIf(x.task_id, x.task_id IS NOT NULL AND x.task_status_cd IN (1,2)) AS main_execution
FROM tmp.dim_basic_filters f
INNER JOIN
(
    SELECT
        if(bitTest(mask,0),'0',if(ifNull(smo_cd,'')='','0',substring(smo_cd,1,2))) AS f_region_smo_cd,
        if(bitTest(mask,1),'0',toString(ifNull(profile_cd,0))) AS f_profile_cd,
        if(bitTest(mask,2),'0',toString(ifNull(group_cd,0))) AS f_dn_group_cd,
        toYear(in_dt) AS f_y,
        0 AS f_q,
        if(bitTest(mask,3),'0',if(ifNull(smo_cd,'')='','0',smo_cd)) AS f_smo,
        if(bitTest(mask,4),'0',if(ifNull(attach_mo_cd,'')='','0',attach_mo_cd)) AS f_mo,
        disp_id,
        mpioip,
        out_dt,
        death_dt,
        task_id,
        task_status_cd
    FROM tmp.for_basic_metrics
    ARRAY JOIN range(32) AS mask
) x
ON  x.f_region_smo_cd = f.f_region_smo_cd
AND x.f_profile_cd = f.f_profile_cd
AND x.f_dn_group_cd = f.f_dn_group_cd
AND x.f_y = f.f_y
AND x.f_q = f.f_q
AND x.f_smo = f.f_smo
AND x.f_mo = f.f_mo
GROUP BY
    f.f_region_smo_cd,
    f.f_profile_cd,
    f.f_dn_group_cd,
    f.f_y,
    f.f_q,
    f.f_smo,
    f.f_mo;
