-- Migration for existing environments.
-- New installations should create dashboard-compatible rating tables directly from create_cube_all.

RENAME TABLE IF EXISTS cube.rating_mo TO cube.mo_rating;
RENAME TABLE IF EXISTS cube.rating_smo TO cube.region_rating;
RENAME TABLE IF EXISTS cube.rating_region TO cube.smo_rating;
