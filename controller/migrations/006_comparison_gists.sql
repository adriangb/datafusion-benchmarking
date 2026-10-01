-- Persist the link before publishing the final comment. The existing finish_key
-- also recovers a Gist whose creation response (or this write) was lost.
ALTER TABLE sharded_runs ADD COLUMN comparison_gist_url TEXT;
