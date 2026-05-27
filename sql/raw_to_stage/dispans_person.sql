CREATE TABLE stage.dispans_person1 AS stage.dispans_person;

INSERT INTO stage.dispans_person1
(
    mpioip,
    birth_dt,
    death_dt,
    pol_cd,
    attach_mo_cd,
    ok_region_cd,
    smo_region_cd,
    smo_cd
)
SELECT
    mpioip,
    birth_dt,
    death_dt,
    pol_cd,
    attach_mo_cd,
    ok_region_cd,
    smo_region_cd,
    smo_cd
FROM
(
    SELECT
        a.mpi_oip AS mpioip,
        a.birth_date AS birth_dt,
        a.deceased_date AS death_dt,
        toInt32(
            splitByChar('/', a.gender_reference)[2]
        ) AS pol_cd,
        toInt32(
            splitByChar('/', b.mo_reference)[2]
        ) AS attach_mo_cd,
        toInt32(
            splitByChar('/', c.tfoms_reference)[2]
        ) AS ok_region_cd,
        toInt32(
            substring(
                splitByChar('/', c.smo_reference)[2], 1,2
            )
        ) AS smo_region_cd,
        toInt32(
            splitByChar('/', c.smo_reference)[2]
        ) AS smo_cd,
        row_number() OVER (
            PARTITION BY a.id
            ORDER BY a.created_at DESC
        ) AS rn
    FROM raw.event_insured_person a
    LEFT JOIN raw.event_mo_attachment b
        ON a.id = b.insured_person_id
    LEFT JOIN raw.event_oms_policy c
        ON a.id = c.insured_person_id
)
WHERE rn = 1;
