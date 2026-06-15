# Raw → Stage

Раздел содержит скрипты формирования stage-слоя из raw-таблиц.

## Назначение

-   преобразование данных raw-слоя в предметные сущности;
-   нормализация атрибутов;
-   удаление дублей;
-   подготовка данных для построения кубов.

## Реализованные таблицы

-   stage.event_task
-   stage.dispans_person
-   stage.disp_info
-   stage.task_usl
-   stage.dispensary_plan
-   stage.inform_task
-   stage.inform_rslt
-   stage.lk_inform

## Структура каталога

-   ddl.sql --- CREATE TABLE для stage-таблиц;
-   mapping.md --- описание маппинга raw → stage;
-   sql/\* --- INSERT-скрипты наполнения.
