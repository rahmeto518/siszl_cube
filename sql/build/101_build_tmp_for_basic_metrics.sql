TRUNCATE TABLE tmp.for_basic_metrics;

INSERT INTO tmp.for_basic_metrics
SELECT
    di.id AS disp_id,
    di.mpioip,
    di.group_cd,
    di.ds_cd,
    di.in_dt,
    di.out_dt,
    p.enp,
    p.birth_dt,
    p.death_dt,
    p.pol_cd,
    p.ok_region_cd,
    p.smo_region_cd,
    p.smo_cd,
    p.attach_mo_cd,
    t.task_id,
    dp.plan_id,
    dp.profile_cd,
    t.task_status_cd,
    t.event_type_cd,
    t.task_start,
    t.task_end,
    t.closed_dt,
    t.insert_dttm,
    t.update_dttm
FROM stage.disp_info di
LEFT JOIN stage.dispensary_plan dp
    ON dp.episod_id = toString(di.id)
LEFT JOIN stage.event_task t
    ON t.plan_id = dp.plan_id
LEFT JOIN stage.dispans_person p
    ON p.mpioip = di.mpioip;
