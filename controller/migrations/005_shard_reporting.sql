CREATE INDEX IF NOT EXISTS idx_jobs_comment_id ON benchmark_jobs(comment_id);

CREATE TABLE sharded_runs (
    comment_id INTEGER NOT NULL,
    benchmarks TEXT NOT NULL,
    started_comment_id INTEGER,
    finished_comment_id INTEGER,
    -- Unpredictable markers prevent another PR commenter from spoofing a
    -- not-yet-published notification and suppressing the bot's real report.
    start_key TEXT NOT NULL DEFAULT (lower(hex(randomblob(16)))),
    finish_key TEXT NOT NULL DEFAULT (lower(hex(randomblob(16)))),
    PRIMARY KEY (comment_id, benchmarks)
);

CREATE TABLE shard_results (
    job_id INTEGER PRIMARY KEY REFERENCES benchmark_jobs(id) ON DELETE CASCADE,
    result_json TEXT NOT NULL
);

CREATE TABLE runner_metadata (
    job_id INTEGER PRIMARY KEY REFERENCES benchmark_jobs(id) ON DELETE CASCADE,
    info_json TEXT NOT NULL
);

-- Legacy workers own their start comments. Do not re-notify terminal targets,
-- even when another target from the same request is still running. In-flight
-- workers that cannot submit exports receive an incomplete-result notification.
INSERT INTO sharded_runs (comment_id, benchmarks, started_comment_id, finished_comment_id)
SELECT comment_id, benchmarks, 0,
       CASE WHEN SUM(status IN ('pending', 'running')) = 0 THEN 0 ELSE NULL END
FROM benchmark_jobs WHERE job_type = 'arrow_criterion' GROUP BY comment_id, benchmarks;
