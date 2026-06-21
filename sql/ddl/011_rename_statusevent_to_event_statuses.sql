-- Migration for existing environments.
-- New installations should create cube.event_statuses directly from status_event_ddl.sql.

RENAME TABLE IF EXISTS cube.statusevent TO cube.event_statuses;

ALTER TABLE cube.event_statuses
    RENAME COLUMN IF EXISTS done_task_count_v TO done_task_countV,
    RENAME COLUMN IF EXISTS done_task_percent_v TO done_task_percentV,
    RENAME COLUMN IF EXISTS assumpt_task_count_v TO assumpt_task_countV,
    RENAME COLUMN IF EXISTS assumpt_task_percent_v TO assumpt_task_percentV,
    RENAME COLUMN IF EXISTS plan_task_count_v TO plan_task_countV,
    RENAME COLUMN IF EXISTS plan_task_percent_v TO plan_task_percentV,
    RENAME COLUMN IF EXISTS failed_task_count_v TO failed_task_countV,
    RENAME COLUMN IF EXISTS failed_task_percent_v TO failed_task_percentV;
