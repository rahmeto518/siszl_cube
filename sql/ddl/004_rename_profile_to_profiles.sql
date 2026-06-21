-- Migration for existing environments.
-- New installations should create cube.profiles directly from profile_ddl_new.sql.

RENAME TABLE IF EXISTS cube.profile TO cube.profiles;
