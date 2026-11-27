-- Interleaved baseline/branch rounds requested by the trigger (`rounds: N`).
-- NULL means a single round.
ALTER TABLE benchmark_jobs ADD COLUMN rounds INTEGER;
