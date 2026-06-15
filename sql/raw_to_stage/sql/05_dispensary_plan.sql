INSERT INTO stage.dispensary_plan
(
    mpioip,
    plan_id,
    status_plane,
    episod_id,
    profile_cd,
    start_date,
    end_date
)
SELECT
    mpi_oip AS mpioip,
    toUUID(id) AS plan_id,
    toInt32(splitByChar('/', execution_state_reference)[2]) AS status_plane,
    dispensary_episode_id AS episod_id,
    toInt32(splitByChar('/', medical_profile_reference)[2]) AS profile_cd,
    period_start AS start_date,
    period_end AS end_date
FROM raw.event_dispensary_plan
WHERE
    toUUIDOrNull(id) IS NOT NULL
    AND period_start IS NOT NULL;
