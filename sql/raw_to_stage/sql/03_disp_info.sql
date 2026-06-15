INSERT INTO stage.disp_info
(
    cmp_id,
    group_cd,
    ds_cd,
    mo_cd,
    in_dt,
    out_dt,
    mpioip
)
SELECT
    cmp_id,
    group_cd,
    ds_cd,
    mo_cd,
    in_dt,
    out_dt,
    mpioip
FROM
(
    SELECT
        id AS cmp_id,
        toInt32(splitByChar('/', diagnosis_group_reference)[2]) AS group_cd,
        toInt32(splitByChar('/', diagnosis_reference)[2]) AS ds_cd,
        toInt32(splitByChar('/', mo_reference)[2]) AS mo_cd,
        period_start AS in_dt,
        period_end AS out_dt,
        mpi_oip AS mpioip,
        row_number() OVER (
            PARTITION BY id
            ORDER BY created_at DESC
        ) AS rn
    FROM raw.event_dispensary_episode
)
WHERE rn = 1;
