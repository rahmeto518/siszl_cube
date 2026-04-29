DROP TABLE IF EXISTS tmp.for_basic_metrics;

CREATE TABLE tmp.for_basic_metrics
(
    disp_id Int64,
    mpioip String,
    group_cd Int32,
    ds_cd String,
    in_dt Date,
    out_dt Nullable(Date),
    enp String,
    birth_dt Date,
    death_dt Nullable(Date),
    pol_cd Int64,
    ok_region_cd String,
    smo_region_cd String,
    smo_cd String,
    attach_mo_cd String,
    task_id UUID,
    plan_id UUID,
    profile_cd Int32,
    task_status_cd Int32,
    event_type_cd Int32,
    task_start Date,
    task_end Date,
    closed_dt Nullable(Date),
    insert_dttm DateTime64(3),
    update_dttm DateTime64(3)
)
ENGINE = MergeTree
ORDER BY (mpioip, disp_id);
