-- Migration for existing environments.
-- New installations should create cube.task_inform directly from create_inform_task.

RENAME TABLE IF EXISTS cube.inform_task TO cube.task_inform;
