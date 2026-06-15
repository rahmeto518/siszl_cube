INSERT INTO stage.lk_inform
(
    mpioip,
    last_login_at
)
SELECT DISTINCT
    mpi_oip AS mpioip,
    last_login_at
FROM raw.person_info;
