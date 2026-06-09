CREATE TABLE IF NOT EXISTS cube.statusevent
(
    f_region_smo_cd String,
    f_profile_cd String,
    f_dn_group_cd String,
    f_y Int32,
    f_q Int32,
    f_smo String,
    f_mo String,
    done_task_count_v UInt64,
    done_task_percent_v Decimal(18, 2),
    assumpt_task_count_v UInt64,
    assumpt_task_percent_v Decimal(18, 2),
    plan_task_count_v UInt64,
    plan_task_percent_v Decimal(18, 2),
    failed_task_count_v UInt64,
    failed_task_percent_v Decimal(18, 2),
    all_tasks UInt64,
    insert_dttm DateTime64(3, 'Europe/Moscow'),
    update_dttm DateTime64(3, 'Europe/Moscow')
)
ENGINE = MergeTree
ORDER BY
(
    f_region_smo_cd,
    f_profile_cd,
    f_dn_group_cd,
    f_y,
    f_q,
    f_smo,
    f_mo
);


CREATE TABLE IF NOT EXISTS tmp.statusevent_task_agg
(
    f_region_smo_cd String,
    f_profile_cd String,
    f_dn_group_cd String,
    f_y Int32,
    f_q Int32,
    f_smo String,
    f_mo String,
    task_status_cd Int32,
    task_cnt UInt64
)
ENGINE = MergeTree
ORDER BY
(
    f_region_smo_cd,
    f_profile_cd,
    f_dn_group_cd,
    f_y,
    f_q,
    f_smo,
    f_mo,
    task_status_cd
);
