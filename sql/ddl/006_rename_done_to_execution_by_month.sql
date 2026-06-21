-- Migration for existing environments.
-- New installations should create cube.execution_by_month directly from cube_done.sql.

RENAME TABLE IF EXISTS cube.done TO cube.execution_by_month;
