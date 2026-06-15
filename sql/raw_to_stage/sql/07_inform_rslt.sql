INSERT INTO stage.inform_rslt
(
    id,
    task_id,
    result_code,
    reaction_code,
    communication_method_code,
    smo_code,
    created_at
)
SELECT
    tm_result_id AS id,
    tm_task_id AS task_id,
    tm_result_cd AS result_code,
    tm_reaction_cd AS reaction_code,
    communication_method_cd AS communication_method_code,
    smo_executor_cd AS smo_code,
    created_dttm AS created_at
FROM raw.dn_inform_rslt;
