-- Per-job random token used by the runner pod to authenticate to the
-- controller's POST /jobs/{id}/comment endpoint. The runner has no direct
-- GitHub credentials; the controller posts comments on its behalf.
ALTER TABLE benchmark_jobs ADD COLUMN runner_token TEXT;

-- Supports per-user concurrency cap queries (count pending/running per login).
CREATE INDEX IF NOT EXISTS idx_jobs_login_status ON benchmark_jobs(login, status);
