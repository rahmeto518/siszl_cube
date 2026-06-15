-- ============================================================
-- Raw → Stage DDL
-- ============================================================

CREATE TABLE stage.event_task
(
    mpioip String,
    task_id UUID,
    plan_id UUID,
    task_status_cd Int32,
    event_type_cd Int32,
    task_start Date,
    task_end Date,
    closed_dt Nullable(Date),
    insert_dttm DateTime64(3, 'Europe/Moscow'),
    update_dttm DateTime64(3, 'Europe/Moscow')
)
ENGINE = MergeTree
ORDER BY (plan_id, task_start, task_end, task_id)
SETTINGS index_granularity = 8192;

CREATE TABLE stage.dispans_person
(
    mpioip String,
    birth_dt Date,
    death_dt Nullable(Date),
    pol_cd Int32,
    attach_mo_cd Int32,
    ok_region_cd Int32,
    smo_region_cd Int32,
    smo_cd Int32
)
ENGINE = MergeTree
ORDER BY (mpioip)
SETTINGS index_granularity = 8192;

CREATE TABLE stage.disp_info
(
    cmp_id String,
    group_cd Int32,
    ds_cd Int32,
    mo_cd Int32,
    in_dt Date,
    out_dt Nullable(Date),
    mpioip String
)
ENGINE = MergeTree
ORDER BY (cmp_id, mpioip)
SETTINGS index_granularity = 8192;

CREATE TABLE stage.task_usl
(
    mpioip String,
    task_id UUID,
    usl_id String
)
ENGINE = MergeTree
ORDER BY (task_id, usl_id)
SETTINGS index_granularity = 8192;

CREATE TABLE stage.dispensary_plan
(
    mpioip String,
    plan_id UUID,
    status_plane Int32,
    episod_id String,
    profile_cd Int32,
    start_date Date,
    end_date Nullable(Date)
)
ENGINE = MergeTree
ORDER BY (plan_id, mpioip, episod_id)
SETTINGS index_granularity = 8192;

CREATE TABLE stage.inform_task
(
    tm_task_id UUID,
    tm_task_type_cd String,
    episode_type_cd String,
    tm_status_cd String,
    mpioip String,
    ds_cd String,
    end_dt Date,
    mo_cd String,
    planned_start_dt Nullable(Date),
    planned_end_dt Nullable(Date),
    group_cd Int32,
    disp_type_cd Nullable(String),
    gender_code String,
    full_age Int32,
    smo_executor_cd String,
    tfoms_cd String,
    region_smo_cd String,
    region_mo_cd String,
    smo_user_id String,
    created_dt DateTime,
    updated_dt DateTime,
    tm_closed_dt Nullable(Date),
    creation_type Nullable(String),
    logical_id Nullable(String),
    comment Nullable(String),
    episode_cmp_id Nullable(Int64),
    tm_event_type Nullable(String),
    profile_cd Int32
)
ENGINE = MergeTree
ORDER BY (tm_task_id, mpioip, created_dt)
SETTINGS index_granularity = 8192;

CREATE TABLE stage.inform_rslt
(
    id UUID,
    task_id UUID,
    result_code String,
    reaction_code Nullable(String),
    communication_method_code String,
    smo_code String,
    created_at DateTime
)
ENGINE = MergeTree
ORDER BY (created_at, id, task_id, result_code)
SETTINGS index_granularity = 8192;

CREATE TABLE stage.lk_inform
(
    mpioip String,
    last_login_at Nullable(Date)
)
ENGINE = MergeTree
ORDER BY (mpioip)
SETTINGS index_granularity = 8192;
