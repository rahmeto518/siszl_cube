CREATE TABLE IF NOT EXISTS cube.profiles
(
    f_region_smo_cd String,
    f_profile_cd String,
    f_dn_group_cd String,
    f_y Int32,
    f_q Int32,
    f_smo String,
    f_mo String,
    bsk_tasks_count_v UInt64,
    zno_tasks_count_v UInt64,
    bsk_done_task_count_v UInt64,
    bsk_done_task_percent_v Decimal(18, 2),
    bsk_assumpt_task_count_v UInt64,
    bsk_assumpt_task_percent_v Decimal(18, 2),
    bsk_plan_task_count_v UInt64,
    bsk_plan_task_percent_v Decimal(18, 2),
    bsk_failed_task_count_v UInt64,
    bsk_failed_task_percent_v Decimal(18, 2),
    zno_done_task_count_v UInt64,
    zno_done_task_percent_v Decimal(18, 2),
    zno_assumpt_task_count_v UInt64,
    zno_assumpt_task_percent_v Decimal(18, 2),
    zno_plan_task_count_v UInt64,
    zno_plan_task_percent_v Decimal(18, 2),
    zno_failed_task_count_v UInt64,
    zno_failed_task_percent_v Decimal(18, 2),
    endo_tasks_count_v UInt64,
    endo_done_task_count_v UInt64,
    endo_done_task_percent_v Decimal(18, 2),
    endo_assumpt_task_count_v UInt64,
    endo_assumpt_task_percent_v Decimal(18, 2),
    endo_plan_task_count_v UInt64,
    endo_plan_task_percent_v Decimal(18, 2),
    endo_failed_task_count_v UInt64,
    endo_failed_task_percent_v Decimal(18, 2),
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
CREATE TABLE IF NOT EXISTS tmp.profile_task_agg
(
    f_region_smo_cd String,
    f_profile_cd String,
    f_dn_group_cd String,
    f_y Int32,
    f_q Int32,
    f_smo String,
    f_mo String,
    profile_cd Int32,
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
    profile_cd,
    task_status_cd
);


