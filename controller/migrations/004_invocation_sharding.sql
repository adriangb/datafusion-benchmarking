-- Existing rows remain unsharded. The assignment version is a compatibility contract.
ALTER TABLE benchmark_jobs ADD COLUMN effective_shards INTEGER NOT NULL DEFAULT 1 CHECK (effective_shards BETWEEN 1 AND 8);
ALTER TABLE benchmark_jobs ADD COLUMN shard_index INTEGER NOT NULL DEFAULT 0 CHECK (shard_index >= 0 AND shard_index < effective_shards);
ALTER TABLE benchmark_jobs ADD COLUMN assignment_version TEXT;
ALTER TABLE benchmark_jobs ADD COLUMN resolved_source_json TEXT;
