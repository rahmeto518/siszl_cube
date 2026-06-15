# Raw → Stage

Раздел содержит скрипты формирования stage-слоя из raw-таблиц.

## Назначение

Stage-слой предназначен для:

- преобразования данных raw-слоя в предметные сущности;
- нормализации атрибутов;
- удаления дублей;
- подготовки данных для дальнейшего расчета кубов.

## Реализованные таблицы

| Stage таблица | Источник |
|---------------|----------|
| stage.event_task | raw.event_task |
| stage.dispans_person | raw.event_insured_person + raw.event_mo_attachment + raw.event_oms_policy |
| stage.disp_info | raw.event_dispensary_episode |
| stage.task_usl | raw.event_task |
| stage.dispensary_plan | raw.event_dispensary_plan |
| stage.inform_task | raw.dn_inform_task |
| stage.inform_rslt | raw.dn_inform_rslt |
| stage.lk_inform | raw.person_info |

## Структура каталога

```text
raw_to_stage/
├── README.md
├── ddl.sql
├── mapping.md
└── sql/
    ├── 01_event_task.sql
    ├── 02_dispans_person.sql
    ├── 03_disp_info.sql
    ├── 04_task_usl.sql
    ├── 05_dispensary_plan.sql
    ├── 06_inform_task.sql
    ├── 07_inform_rslt.sql
    └── 08_lk_inform.sql
```

## Порядок запуска

1. Выполнить `ddl.sql`.
2. Выполнить INSERT-скрипты из каталога `sql/` по порядку.
