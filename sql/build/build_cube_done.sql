TRUNCATE TABLE tmp.done_task_month_source;

INSERT INTO tmp.done_task_month_source
SELECT DISTINCT
    task_id,
    task_status_cd,
    assumeNotNull(closed_dt) AS closed_dt,
    toYear(assumeNotNull(closed_dt)) AS closed_y,
    toQuarter(assumeNotNull(closed_dt)) AS closed_q,
    toMonth(assumeNotNull(closed_dt)) AS closed_m,
    if(ifNull(smo_cd, '') = '', '0', substring(smo_cd, 1, 2)) AS region_smo_cd,
    toString(ifNull(profile_cd, 0)) AS profile_cd,
    toString(ifNull(group_cd, 0)) AS dn_group_cd,
    if(ifNull(smo_cd, '') = '', '0', smo_cd) AS smo_cd,
    if(ifNull(attach_mo_cd, '') = '', '0', attach_mo_cd) AS mo_cd
FROM tmp.for_basic_metrics
WHERE task_id IS NOT NULL
  AND closed_dt IS NOT NULL
  AND task_status_cd IN (1, 2)
  AND toYear(assumeNotNull(closed_dt)) BETWEEN 2018 AND 2027;

TRUNCATE TABLE cube.execution_by_month;

INSERT INTO cube.execution_by_month
WITH prepared AS
(
    SELECT
        if(bitTest(mask, 0), '0', region_smo_cd) AS f_region_smo_cd,
        if(bitTest(mask, 1), '0', profile_cd) AS f_profile_cd,
        if(bitTest(mask, 2), '0', dn_group_cd) AS f_dn_group_cd,
        if(metric_kind = 'p', closed_y + 1, closed_y) AS f_y,
        closed_q AS f_q,
        if(bitTest(mask, 3), '0', smo_cd) AS f_smo,
        if(bitTest(mask, 4), '0', mo_cd) AS f_mo,
        metric_kind,
        task_id,
        task_status_cd,
        closed_m
    FROM tmp.done_task_month_source
    CROSS JOIN
    (
        SELECT arrayJoin(['p', 'c']) AS metric_kind
    ) AS k
    CROSS JOIN
    (
        SELECT arrayJoin(range(32)) AS mask
    ) AS m
    WHERE if(metric_kind = 'p', closed_y + 1, closed_y) BETWEEN 2019 AND 2027
)
SELECT
    f_region_smo_cd,
    f_profile_cd,
    f_dn_group_cd,
    f_y,
    f_q,
    f_smo,
    f_mo,
    uniqExactIf(task_id, metric_kind = 'p' AND task_status_cd = 1 AND closed_m = 1) AS p_done_m1_v,
    uniqExactIf(task_id, metric_kind = 'p' AND task_status_cd = 2 AND closed_m = 1) AS p_assumpt_m1_v,
    uniqExactIf(task_id, metric_kind = 'c' AND task_status_cd = 1 AND closed_m = 1) AS c_done_m1_v,
    uniqExactIf(task_id, metric_kind = 'c' AND task_status_cd = 2 AND closed_m = 1) AS c_assumpt_m1_v,
    uniqExactIf(task_id, metric_kind = 'p' AND task_status_cd = 1 AND closed_m = 2) AS p_done_m2_v,
    uniqExactIf(task_id, metric_kind = 'p' AND task_status_cd = 2 AND closed_m = 2) AS p_assumpt_m2_v,
    uniqExactIf(task_id, metric_kind = 'c' AND task_status_cd = 1 AND closed_m = 2) AS c_done_m2_v,
    uniqExactIf(task_id, metric_kind = 'c' AND task_status_cd = 2 AND closed_m = 2) AS c_assumpt_m2_v,
    uniqExactIf(task_id, metric_kind = 'p' AND task_status_cd = 1 AND closed_m = 3) AS p_done_m3_v,
    uniqExactIf(task_id, metric_kind = 'p' AND task_status_cd = 2 AND closed_m = 3) AS p_assumpt_m3_v,
    uniqExactIf(task_id, metric_kind = 'c' AND task_status_cd = 1 AND closed_m = 3) AS c_done_m3_v,
    uniqExactIf(task_id, metric_kind = 'c' AND task_status_cd = 2 AND closed_m = 3) AS c_assumpt_m3_v,
    uniqExactIf(task_id, metric_kind = 'p' AND task_status_cd = 1 AND closed_m = 4) AS p_done_m4_v,
    uniqExactIf(task_id, metric_kind = 'p' AND task_status_cd = 2 AND closed_m = 4) AS p_assumpt_m4_v,
    uniqExactIf(task_id, metric_kind = 'c' AND task_status_cd = 1 AND closed_m = 4) AS c_done_m4_v,
    uniqExactIf(task_id, metric_kind = 'c' AND task_status_cd = 2 AND closed_m = 4) AS c_assumpt_m4_v,
    uniqExactIf(task_id, metric_kind = 'p' AND task_status_cd = 1 AND closed_m = 5) AS p_done_m5_v,
    uniqExactIf(task_id, metric_kind = 'p' AND task_status_cd = 2 AND closed_m = 5) AS p_assumpt_m5_v,
    uniqExactIf(task_id, metric_kind = 'c' AND task_status_cd = 1 AND closed_m = 5) AS c_done_m5_v,
    uniqExactIf(task_id, metric_kind = 'c' AND task_status_cd = 2 AND closed_m = 5) AS c_assumpt_m5_v,
    uniqExactIf(task_id, metric_kind = 'p' AND task_status_cd = 1 AND closed_m = 6) AS p_done_m6_v,
    uniqExactIf(task_id, metric_kind = 'p' AND task_status_cd = 2 AND closed_m = 6) AS p_assumpt_m6_v,
    uniqExactIf(task_id, metric_kind = 'c' AND task_status_cd = 1 AND closed_m = 6) AS c_done_m6_v,
    uniqExactIf(task_id, metric_kind = 'c' AND task_status_cd = 2 AND closed_m = 6) AS c_assumpt_m6_v,
    uniqExactIf(task_id, metric_kind = 'p' AND task_status_cd = 1 AND closed_m = 7) AS p_done_m7_v,
    uniqExactIf(task_id, metric_kind = 'p' AND task_status_cd = 2 AND closed_m = 7) AS p_assumpt_m7_v,
    uniqExactIf(task_id, metric_kind = 'c' AND task_status_cd = 1 AND closed_m = 7) AS c_done_m7_v,
    uniqExactIf(task_id, metric_kind = 'c' AND task_status_cd = 2 AND closed_m = 7) AS c_assumpt_m7_v,
    uniqExactIf(task_id, metric_kind = 'p' AND task_status_cd = 1 AND closed_m = 8) AS p_done_m8_v,
    uniqExactIf(task_id, metric_kind = 'p' AND task_status_cd = 2 AND closed_m = 8) AS p_assumpt_m8_v,
    uniqExactIf(task_id, metric_kind = 'c' AND task_status_cd = 1 AND closed_m = 8) AS c_done_m8_v,
    uniqExactIf(task_id, metric_kind = 'c' AND task_status_cd = 2 AND closed_m = 8) AS c_assumpt_m8_v,
    uniqExactIf(task_id, metric_kind = 'p' AND task_status_cd = 1 AND closed_m = 9) AS p_done_m9_v,
    uniqExactIf(task_id, metric_kind = 'p' AND task_status_cd = 2 AND closed_m = 9) AS p_assumpt_m9_v,
    uniqExactIf(task_id, metric_kind = 'c' AND task_status_cd = 1 AND closed_m = 9) AS c_done_m9_v,
    uniqExactIf(task_id, metric_kind = 'c' AND task_status_cd = 2 AND closed_m = 9) AS c_assumpt_m9_v,
    uniqExactIf(task_id, metric_kind = 'p' AND task_status_cd = 1 AND closed_m = 10) AS p_done_m10_v,
    uniqExactIf(task_id, metric_kind = 'p' AND task_status_cd = 2 AND closed_m = 10) AS p_assumpt_m10_v,
    uniqExactIf(task_id, metric_kind = 'c' AND task_status_cd = 1 AND closed_m = 10) AS c_done_m10_v,
    uniqExactIf(task_id, metric_kind = 'c' AND task_status_cd = 2 AND closed_m = 10) AS c_assumpt_m10_v,
    uniqExactIf(task_id, metric_kind = 'p' AND task_status_cd = 1 AND closed_m = 11) AS p_done_m11_v,
    uniqExactIf(task_id, metric_kind = 'p' AND task_status_cd = 2 AND closed_m = 11) AS p_assumpt_m11_v,
    uniqExactIf(task_id, metric_kind = 'c' AND task_status_cd = 1 AND closed_m = 11) AS c_done_m11_v,
    uniqExactIf(task_id, metric_kind = 'c' AND task_status_cd = 2 AND closed_m = 11) AS c_assumpt_m11_v,
    uniqExactIf(task_id, metric_kind = 'p' AND task_status_cd = 1 AND closed_m = 12) AS p_done_m12_v,
    uniqExactIf(task_id, metric_kind = 'p' AND task_status_cd = 2 AND closed_m = 12) AS p_assumpt_m12_v,
    uniqExactIf(task_id, metric_kind = 'c' AND task_status_cd = 1 AND closed_m = 12) AS c_done_m12_v,
    uniqExactIf(task_id, metric_kind = 'c' AND task_status_cd = 2 AND closed_m = 12) AS c_assumpt_m12_v,
    now64(3, 'Europe/Moscow') AS insert_dttm,
    now64(3, 'Europe/Moscow') AS update_dttm
FROM prepared
GROUP BY
    f_region_smo_cd,
    f_profile_cd,
    f_dn_group_cd,
    f_y,
    f_q,
    f_smo,
    f_mo
HAVING
    (p_done_m1_v +
        p_assumpt_m1_v +
        c_done_m1_v +
        c_assumpt_m1_v +
        p_done_m2_v +
        p_assumpt_m2_v +
        c_done_m2_v +
        c_assumpt_m2_v +
        p_done_m3_v +
        p_assumpt_m3_v +
        c_done_m3_v +
        c_assumpt_m3_v +
        p_done_m4_v +
        p_assumpt_m4_v +
        c_done_m4_v +
        c_assumpt_m4_v +
        p_done_m5_v +
        p_assumpt_m5_v +
        c_done_m5_v +
        c_assumpt_m5_v +
        p_done_m6_v +
        p_assumpt_m6_v +
        c_done_m6_v +
        c_assumpt_m6_v +
        p_done_m7_v +
        p_assumpt_m7_v +
        c_done_m7_v +
        c_assumpt_m7_v +
        p_done_m8_v +
        p_assumpt_m8_v +
        c_done_m8_v +
        c_assumpt_m8_v +
        p_done_m9_v +
        p_assumpt_m9_v +
        c_done_m9_v +
        c_assumpt_m9_v +
        p_done_m10_v +
        p_assumpt_m10_v +
        c_done_m10_v +
        c_assumpt_m10_v +
        p_done_m11_v +
        p_assumpt_m11_v +
        c_done_m11_v +
        c_assumpt_m11_v +
        p_done_m12_v +
        p_assumpt_m12_v +
        c_done_m12_v +
        c_assumpt_m12_v) <> 0;
