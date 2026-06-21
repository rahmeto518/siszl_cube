-- Migration for existing environments.
-- New installations should create cube.targets directly from cube_work.

RENAME TABLE IF EXISTS cube.work TO cube.targets;

-- Some older DDL scripts created this cube as cube.work2 instead of cube.work.
-- Run the next statement only if cube.targets does not already exist:
-- RENAME TABLE IF EXISTS cube.work2 TO cube.targets;
