

INSERT INTO stage.event_task1
(
    mpioip,
    task_id,
    plan_id,
    task_status_cd,
    event_type_cd,
    task_start,
    task_end,
    closed_dt,
    insert_dttm,
    update_dttm
)
SELECT
    mpioip,
    task_id,
    plan_id,
    task_status_cd,
    event_type_cd,
    task_start,
    task_end,
    closed_dt,
    insert_dttm,
    update_dttm
FROM
(
    SELECT
        mpi_oip AS mpioip,
        toUUID(id) AS task_id,
        toUUID(dispensary_plan_id) AS plan_id,

        toInt32(splitByChar('/', state_reference)[2]) AS task_status_cd,
        toInt32(splitByChar('/', task_reference)[2]) AS event_type_cd,

        restriction_period_start AS task_start,
        restriction_period_end AS task_end,
        medical_service_completed_date AS closed_dt,

        toTimeZone(created_at, 'Europe/Moscow') AS insert_dttm,
        toTimeZone(updated_at, 'Europe/Moscow') AS update_dttm,

        row_number() OVER (
            PARTITION BY
                toUUID(id),
                toInt32(splitByChar('/', state_reference)[2])
            ORDER BY
                created_at DESC
        ) AS rn
    FROM raw.event_task
    WHERE
        toUUIDOrNull(id) IS NOT NULL
        AND toUUIDOrNull(dispensary_plan_id) IS NOT NULL
        AND restriction_period_start IS NOT NULL
        AND restriction_period_end IS NOT NULL
)
WHERE rn = 1;
