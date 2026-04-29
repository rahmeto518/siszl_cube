DROP TABLE IF EXISTS tmp.dim_basic_filters;

CREATE TABLE tmp.dim_basic_filters
(
    f_region_smo_cd String,
    f_profile_cd String,
    f_dn_group_cd String,
    f_y Int32,
    f_q Int32,
    f_smo String,
    f_mo String
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
