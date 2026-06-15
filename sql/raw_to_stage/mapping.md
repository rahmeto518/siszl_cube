# Mapping Raw → Stage

## stage.event_task

Источник: `raw.event_task`

| raw | stage | Правило |
|-----|-------|---------|
| mpi_oip | mpioip | прямой перенос |
| id | task_id | toUUID(id) |
| dispensary_plan_id | plan_id | toUUID(dispensary_plan_id) |
| state_reference | task_status_cd | число после `/` |
| task_reference | event_type_cd | число после `/` |
| restriction_period_start | task_start | прямой перенос |
| restriction_period_end | task_end | прямой перенос |
| medical_service_completed_date | closed_dt | прямой перенос |
| created_at | insert_dttm | Europe/Moscow |
| updated_at | update_dttm | Europe/Moscow |

Дедупликация: по `(task_id, task_status_cd)`, остается запись с максимальным `insert_dttm`.

## stage.dispans_person

Источники:

- `raw.event_insured_person a`
- `raw.event_mo_attachment b`
- `raw.event_oms_policy c`

Join:

```sql
a.id = b.insured_person_id
a.id = c.insured_person_id
```

| raw | stage | Правило |
|-----|-------|---------|
| a.mpi_oip | mpioip | прямой перенос |
| a.birth_date | birth_dt | прямой перенос |
| a.deceased_date | death_dt | прямой перенос |
| a.gender_reference | pol_cd | число после `/` |
| b.mo_reference | attach_mo_cd | число после `/` |
| c.tfoms_reference | ok_region_cd | число после `/` |
| c.smo_reference | smo_region_cd | первые 2 цифры после `/` |
| c.smo_reference | smo_cd | всё число после `/` |

## stage.disp_info

Источник: `raw.event_dispensary_episode`

| raw | stage | Правило |
|-----|-------|---------|
| id | cmp_id | прямой перенос |
| diagnosis_group_reference | group_cd | число после `/` |
| diagnosis_reference | ds_cd | число после `/` |
| mo_reference | mo_cd | число после `/` |
| period_start | in_dt | прямой перенос |
| period_end | out_dt | прямой перенос |
| mpi_oip | mpioip | прямой перенос |

## stage.task_usl

Источник: `raw.event_task`

| raw | stage |
|-----|-------|
| mpi_oip | mpioip |
| id | task_id |
| execution_result_medical_service_cmp_id | usl_id |

## stage.dispensary_plan

Источник: `raw.event_dispensary_plan`

| raw | stage | Правило |
|-----|-------|---------|
| mpi_oip | mpioip | прямой перенос |
| id | plan_id | toUUID(id) |
| execution_state_reference | status_plane | число после `/` |
| dispensary_episode_id | episod_id | прямой перенос |
| medical_profile_reference | profile_cd | число после `/` |
| period_start | start_date | прямой перенос |
| period_end | end_date | прямой перенос |

## stage.inform_task

Источник: `raw.dn_inform_task`

Итоговый вариант зафиксирован в `ddl.sql` и `sql/06_inform_task.sql`.

## stage.inform_rslt

Источник: `raw.dn_inform_rslt`

| raw | stage |
|-----|-------|
| tm_result_id | id |
| tm_task_id | task_id |
| tm_result_cd | result_code |
| tm_reaction_cd | reaction_code |
| communication_method_cd | communication_method_code |
| smo_executor_cd | smo_code |
| created_dttm | created_at |

## stage.lk_inform

Источник: `raw.person_info`

| raw | stage |
|-----|-------|
| mpi_oip | mpioip |
| last_login_at | last_login_at |

Дедупликация: `SELECT DISTINCT` по `(mpioip, last_login_at)`.
