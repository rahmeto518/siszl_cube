INSERT INTO stage.task_usl
(
    mpioip,
    task_id,
    usl_id
)
SELECT
    mpi_oip AS mpioip,
    toUUID(id) AS task_id,
    execution_result_medical_service_cmp_id AS usl_id
FROM raw.event_task
WHERE
    toUUIDOrNull(id) IS NOT NULL
    AND execution_result_medical_service_cmp_id IS NOT NULL;
