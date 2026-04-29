DROP TABLE IF EXISTS cube.basic_metrics;

CREATE TABLE cube.basic_metrics
(
    f_region_smo_cd String,
    f_profile_cd String,
    f_dn_group_cd String,
    f_y Int32,
    f_q Int32,
    f_smo String,
    f_mo String,
    sl_dn_cnt UInt64,
    zl_on_dn_total UInt64,
    death_new UInt64,
    all_tasks UInt64,
    main_execution UInt64
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
