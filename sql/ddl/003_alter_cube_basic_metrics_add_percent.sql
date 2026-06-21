ALTER TABLE cube.basic_metrics
    ADD COLUMN IF NOT EXISTS death_new_percent UInt64 AFTER death_new,
    ADD COLUMN IF NOT EXISTS main_execution_percent UInt64 AFTER main_execution;
