//! Minimal TCP-based HTTP server.
//!
//! Serves Kubernetes liveness/readiness probes plus a narrow
//! `POST /jobs/{id}/comment` endpoint used by benchmark runner pods to post
//! PR comments. The runner has no GitHub credentials of its own; it hands a
//! markdown body to the controller (authenticated with a per-job token
//! injected at pod-creation time) and the controller posts it via
//! [`GitHubClient::post_comment`].

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use sqlx::SqlitePool;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::github::GitHubClient;

const OK: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
const NOT_FOUND: &[u8] = b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
const BAD_REQUEST: &[u8] = b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n";
const UNAUTHORIZED: &[u8] = b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n";
const CONFLICT: &[u8] = b"HTTP/1.1 409 Conflict\r\nContent-Length: 0\r\n\r\n";
const INTERNAL: &[u8] = b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n";
const PAYLOAD_TOO_LARGE: &[u8] = b"HTTP/1.1 413 Payload Too Large\r\nContent-Length: 0\r\n\r\n";
const UNAVAILABLE: &[u8] =
    b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 9\r\n\r\nnot ready";

/// Hard cap on a single POST body. GitHub rejects comments over ~65 KB, so
/// 128 KB is more than enough and prevents a runaway runner from OOM-ing
/// the controller.
const MAX_COMMENT_BYTES: usize = 128 << 10;
const MAX_RESULT_BYTES: usize = 16 << 20;

/// Shared flag set to `true` once both loops have started.
pub type ReadyFlag = Arc<AtomicBool>;

pub fn ready_flag() -> ReadyFlag {
    Arc::new(AtomicBool::new(false))
}

/// Listen on `0.0.0.0:8080` and serve `/healthz`, `/readyz`, and
/// `POST /jobs/{id}/comment`, and `POST /jobs/{id}/result`.
pub async fn serve(token: CancellationToken, ready: ReadyFlag, pool: SqlitePool, gh: GitHubClient) {
    let listener = TcpListener::bind("0.0.0.0:8080")
        .await
        .expect("failed to bind health port 8080");
    info!("http server listening on :8080");

    loop {
        let (stream, _addr) = tokio::select! {
            res = listener.accept() => match res {
                Ok(conn) => conn,
                Err(e) => {
                    tracing::warn!(error = %e, "accept error");
                    continue;
                }
            },
            _ = token.cancelled() => break,
        };

        let ready = ready.clone();
        let pool = pool.clone();
        let gh = gh.clone();
        tokio::spawn(handle_conn(stream, ready, pool, gh));
    }

    info!("http server stopped");
}

async fn handle_conn(mut stream: TcpStream, ready: ReadyFlag, pool: SqlitePool, gh: GitHubClient) {
    let response: Vec<u8> = match read_request(&mut stream).await {
        Ok(Some(req)) => match route(&req, &ready, &pool, &gh).await {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::warn!(error = %e, "handler error");
                INTERNAL.to_vec()
            }
        },
        Ok(None) => BAD_REQUEST.to_vec(),
        Err(RequestError::TooLarge) => PAYLOAD_TOO_LARGE.to_vec(),
        Err(RequestError::Malformed) => BAD_REQUEST.to_vec(),
        Err(RequestError::Io(e)) => {
            tracing::warn!(error = %e, "connection read error");
            return;
        }
    };
    let _ = stream.write_all(&response).await;
    let _ = stream.shutdown().await;
}

async fn route(
    req: &Request,
    ready: &ReadyFlag,
    pool: &SqlitePool,
    gh: &GitHubClient,
) -> anyhow::Result<Vec<u8>> {
    // Liveness
    if req.method == "GET" && req.path == "/healthz" {
        return Ok(OK.to_vec());
    }
    // Readiness
    if req.method == "GET" && req.path == "/readyz" {
        return Ok(if ready.load(Ordering::Relaxed) {
            OK.to_vec()
        } else {
            UNAVAILABLE.to_vec()
        });
    }
    // Authenticated runner callbacks.
    if req.method == "POST" {
        if let Some(job_id) = parse_job_comment_path(&req.path)
            .or_else(|| parse_job_result_path(&req.path))
            .or_else(|| parse_job_info_path(&req.path))
        {
            return handle_job_comment(req, job_id, pool, gh).await;
        }
    }
    Ok(NOT_FOUND.to_vec())
}

async fn handle_job_comment(
    req: &Request,
    job_id: i64,
    pool: &SqlitePool,
    gh: &GitHubClient,
) -> anyhow::Result<Vec<u8>> {
    // Auth: `Authorization: Bearer <token>` must match the row's runner_token.
    let bearer = match req.header("authorization") {
        Some(v) => v,
        None => return Ok(UNAUTHORIZED.to_vec()),
    };
    let supplied = match bearer
        .strip_prefix("Bearer ")
        .or_else(|| bearer.strip_prefix("bearer "))
    {
        Some(t) => t.trim(),
        None => return Ok(UNAUTHORIZED.to_vec()),
    };

    let job: crate::models::BenchmarkJob =
        match sqlx::query_as("SELECT * FROM benchmark_jobs WHERE id = ?")
            .bind(job_id)
            .fetch_optional(pool)
            .await?
        {
            Some(row) => row,
            None => return Ok(NOT_FOUND.to_vec()),
        };
    match &job.runner_token {
        Some(tok) if constant_time_eq(tok.as_bytes(), supplied.as_bytes()) => {}
        _ => return Ok(UNAUTHORIZED.to_vec()),
    }
    if parse_job_result_path(&req.path).is_some() {
        let result: crate::shard_reporting::ShardResult = match serde_json::from_slice(&req.body) {
            Ok(result) => result,
            Err(_) => return Ok(BAD_REQUEST.to_vec()),
        };
        if result.validate(&job).is_err() {
            return Ok(BAD_REQUEST.to_vec());
        }
        return Ok(
            if crate::shard_reporting::store_result(pool, &job, &result).await? {
                OK.to_vec()
            } else {
                CONFLICT.to_vec()
            },
        );
    }
    if req.body.len() > MAX_COMMENT_BYTES {
        return Ok(PAYLOAD_TOO_LARGE.to_vec());
    }
    if parse_job_info_path(&req.path).is_some() {
        if !job.uses_collected_results() {
            return Ok(BAD_REQUEST.to_vec());
        }
        let info: crate::criterion_report::RunnerInfo = match serde_json::from_slice(&req.body) {
            Ok(info) => info,
            Err(_) => return Ok(BAD_REQUEST.to_vec()),
        };
        return Ok(
            if crate::shard_reporting::store_runner_info(pool, job_id, &info).await? {
                OK.to_vec()
            } else {
                CONFLICT.to_vec()
            },
        );
    }
    let status = &job.status;
    // The runner may try to post its initial "running" comment before the
    // reconciler has updated the DB row from `pending` to `running`.
    // Accepting both non-terminal states avoids that startup race while still
    // preventing replay after completion/failure.
    if !status_allows_runner_comment(status) {
        tracing::warn!(job_id, status, "rejecting runner comment in terminal state");
        return Ok(CONFLICT.to_vec());
    }

    // Parse body: {"body": "<markdown>"}. `body` must be `String`, not
    // `&str`: comment bodies contain `\n` etc. which require unescaping
    // and can't be zero-copy borrowed out of the source bytes.
    #[derive(serde::Deserialize)]
    struct CommentReq {
        body: String,
    }
    let payload: CommentReq = match serde_json::from_slice(&req.body) {
        Ok(p) => p,
        Err(_) => return Ok(BAD_REQUEST.to_vec()),
    };
    if payload.body.is_empty() {
        return Ok(BAD_REQUEST.to_vec());
    }

    // Collected results are reported by the reconciler.
    if !job.uses_collected_results() {
        gh.post_comment(&job.repo, job.pr_number, &payload.body)
            .await?;
    }
    Ok(OK.to_vec())
}

fn parse_job_comment_path(path: &str) -> Option<i64> {
    let rest = path.strip_prefix("/jobs/")?;
    let (id, tail) = rest.split_once('/')?;
    if tail != "comment" {
        return None;
    }
    id.parse().ok()
}

fn parse_job_result_path(path: &str) -> Option<i64> {
    let rest = path.strip_prefix("/jobs/")?;
    let (id, tail) = rest.split_once('/')?;
    (tail == "result").then(|| id.parse().ok()).flatten()
}

fn parse_job_info_path(path: &str) -> Option<i64> {
    let rest = path.strip_prefix("/jobs/")?;
    let (id, tail) = rest.split_once('/')?;
    (tail == "info").then(|| id.parse().ok()).flatten()
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

fn status_allows_runner_comment(status: &str) -> bool {
    matches!(status, "pending" | "running")
}

// ── Minimal HTTP/1.1 request parsing ────────────────────────────────

struct Request {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Request {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

enum RequestError {
    Malformed,
    TooLarge,
    Io(std::io::Error),
}

impl From<std::io::Error> for RequestError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

async fn read_request(stream: &mut TcpStream) -> Result<Option<Request>, RequestError> {
    // Read until we have the header terminator, then read Content-Length bytes.
    let mut buf: Vec<u8> = Vec::with_capacity(2048);
    let mut scratch = [0u8; 2048];
    let header_end = loop {
        let n = stream.read(&mut scratch).await?;
        if n == 0 {
            return Ok(None);
        }
        buf.extend_from_slice(&scratch[..n]);
        if let Some(idx) = find_header_end(&buf) {
            if idx > MAX_COMMENT_BYTES {
                return Err(RequestError::TooLarge);
            }
            break idx;
        }
        if buf.len() > MAX_COMMENT_BYTES {
            return Err(RequestError::TooLarge);
        }
    };

    let header_bytes = &buf[..header_end];
    let mut lines = header_bytes
        .split(|&b| b == b'\n')
        .map(|l| l.strip_suffix(b"\r").unwrap_or(l));

    let request_line = lines.next().ok_or(RequestError::Malformed)?;
    let req_str = std::str::from_utf8(request_line).map_err(|_| RequestError::Malformed)?;
    let mut parts = req_str.split_whitespace();
    let method = parts.next().ok_or(RequestError::Malformed)?.to_string();
    let path = parts.next().ok_or(RequestError::Malformed)?.to_string();

    let mut headers = Vec::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let s = std::str::from_utf8(line).map_err(|_| RequestError::Malformed)?;
        if let Some((k, v)) = s.split_once(':') {
            headers.push((k.trim().to_string(), v.trim().to_string()));
        }
    }

    // Determine expected body length
    let content_length: usize = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(0);
    let body_limit = if method == "POST" && parse_job_result_path(&path).is_some() {
        MAX_RESULT_BYTES
    } else {
        MAX_COMMENT_BYTES
    };
    if content_length > body_limit {
        return Err(RequestError::TooLarge);
    }

    // Body starts 4 bytes after header_end (the \r\n\r\n).
    let body_start = header_end + 4;
    let already = buf.len().saturating_sub(body_start);
    let mut body = buf[body_start.min(buf.len())..].to_vec();
    while body.len() < content_length {
        let n = stream.read(&mut scratch).await?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&scratch[..n]);
        if body.len() > body_limit {
            return Err(RequestError::TooLarge);
        }
    }
    if body.len() < content_length {
        return Err(RequestError::Malformed);
    }
    body.truncate(content_length);
    let _ = already; // silence unused warning; body was built from `buf`

    Ok(Some(Request {
        method,
        path,
        headers,
        body,
    }))
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_parse_ok() {
        assert_eq!(parse_job_comment_path("/jobs/42/comment"), Some(42));
        assert_eq!(parse_job_comment_path("/jobs/1/comment"), Some(1));
    }

    #[test]
    fn path_parse_rejects_other() {
        assert_eq!(parse_job_comment_path("/jobs/42"), None);
        assert_eq!(parse_job_comment_path("/jobs/abc/comment"), None);
        assert_eq!(parse_job_comment_path("/jobs/42/other"), None);
        assert_eq!(parse_job_comment_path("/other"), None);
    }

    #[test]
    fn ct_eq_true_on_equal() {
        assert!(constant_time_eq(b"abc", b"abc"));
    }

    #[test]
    fn ct_eq_false_on_different_len() {
        assert!(!constant_time_eq(b"abc", b"abcd"));
    }

    #[test]
    fn ct_eq_false_on_different_bytes() {
        assert!(!constant_time_eq(b"abc", b"abd"));
    }

    #[test]
    fn header_end_finds_boundary() {
        let buf = b"GET / HTTP/1.1\r\nHost: x\r\n\r\nbody";
        let pos = find_header_end(buf).unwrap();
        assert_eq!(&buf[pos..pos + 4], b"\r\n\r\n");
    }

    #[test]
    fn runner_comments_allowed_for_non_terminal_states() {
        assert!(status_allows_runner_comment("pending"));
        assert!(status_allows_runner_comment("running"));
    }

    #[tokio::test]
    async fn shard_endpoint_authentication_idempotency_and_legacy_comment_suppression() {
        use crate::shard_reporting::tests::{github, request, result};
        let pool = crate::db::connect("sqlite::memory:").await.unwrap();
        for count in [1, 2, 4] {
            let jobs = request(&pool, 200 + i64::from(count), count, &["target", "another"]).await;
            crate::db::set_runner_token(&pool, jobs[0].id, "worker-zero")
                .await
                .unwrap();
            crate::db::set_runner_token(&pool, jobs[1].id, "worker-one")
                .await
                .unwrap();
            let (gh, comments, server) = github().await;
            let ready = ready_flag();
            let mut req = Request {
                method: "POST".into(),
                path: format!("/jobs/{}/result", jobs[0].id),
                headers: vec![],
                body: serde_json::to_vec(&result(&jobs[0])).unwrap(),
            };
            assert_eq!(route(&req, &ready, &pool, &gh).await.unwrap(), UNAUTHORIZED);
            req.headers
                .push(("Authorization".into(), "Bearer worker-one".into()));
            assert_eq!(route(&req, &ready, &pool, &gh).await.unwrap(), UNAUTHORIZED);
            req.headers[0].1 = "Bearer worker-zero".into();
            assert_eq!(route(&req, &ready, &pool, &gh).await.unwrap(), OK);
            assert_eq!(route(&req, &ready, &pool, &gh).await.unwrap(), OK);
            let mut invalid = result(&jobs[0]);
            invalid.changed.name = "wrong".into();
            req.body = serde_json::to_vec(&invalid).unwrap();
            assert_eq!(route(&req, &ready, &pool, &gh).await.unwrap(), BAD_REQUEST);
            req.path = format!("/jobs/{}/comment", jobs[0].id);
            req.body = br#"{"body":"legacy shard comment"}"#.to_vec();
            assert_eq!(route(&req, &ready, &pool, &gh).await.unwrap(), OK);
            assert!(comments.lock().await.is_empty());
            crate::db::update_job_status(
                &pool,
                jobs[0].id,
                crate::models::JobStatus::Completed,
                None,
                None,
            )
            .await
            .unwrap();
            assert_eq!(route(&req, &ready, &pool, &gh).await.unwrap(), CONFLICT);
            req.path = format!("/jobs/{}/result", jobs[0].id);
            req.body = serde_json::to_vec(&result(&jobs[0])).unwrap();
            assert_eq!(route(&req, &ready, &pool, &gh).await.unwrap(), OK); // Lost acknowledgement retry.
            server.abort();
        }
    }

    #[tokio::test]
    async fn datafusion_comments_still_forward_directly() {
        use crate::shard_reporting::tests::{github, request};
        let pool = crate::db::connect("sqlite::memory:").await.unwrap();
        let jobs = request(&pool, 201, 1, &["target"]).await;
        sqlx::query("UPDATE benchmark_jobs SET job_type = 'datafusion' WHERE id = ?")
            .bind(jobs[0].id)
            .execute(&pool)
            .await
            .unwrap();
        crate::db::set_runner_token(&pool, jobs[0].id, "token")
            .await
            .unwrap();
        let (gh, comments, server) = github().await;
        let req = Request {
            method: "POST".into(),
            path: format!("/jobs/{}/comment", jobs[0].id),
            headers: vec![("Authorization".into(), "Bearer token".into())],
            body: br#"{"body":"ordinary comment"}"#.to_vec(),
        };
        assert_eq!(route(&req, &ready_flag(), &pool, &gh).await.unwrap(), OK);
        assert_eq!(comments.lock().await.len(), 1);
        assert_eq!(comments.lock().await[0]["body"], "ordinary comment");
        server.abort();
    }

    #[tokio::test]
    async fn metadata_is_authenticated_durable_and_immutable_after_completion() {
        use crate::shard_reporting::tests::{github, request, result};
        let pool = crate::db::connect("sqlite::memory:").await.unwrap();
        let jobs = request(&pool, 301, 1, &["target"]).await;
        let job = &jobs[0];
        crate::db::set_runner_token(&pool, job.id, "token")
            .await
            .unwrap();
        let (gh, comments, server) = github().await;
        let mut info = result(job).info;
        let mut req = Request {
            method: "POST".into(),
            path: format!("/jobs/{}/info", job.id),
            headers: vec![],
            body: serde_json::to_vec(&info).unwrap(),
        };
        assert_eq!(
            route(&req, &ready_flag(), &pool, &gh).await.unwrap(),
            UNAUTHORIZED
        );
        req.headers
            .push(("Authorization".into(), "Bearer token".into()));
        assert_eq!(route(&req, &ready_flag(), &pool, &gh).await.unwrap(), OK);
        info.resource_report = "baseline usage".into();
        req.body = serde_json::to_vec(&info).unwrap();
        assert_eq!(route(&req, &ready_flag(), &pool, &gh).await.unwrap(), OK);
        assert!(comments.lock().await.is_empty());
        crate::db::update_job_status(&pool, job.id, crate::models::JobStatus::Failed, None, None)
            .await
            .unwrap();
        assert_eq!(route(&req, &ready_flag(), &pool, &gh).await.unwrap(), OK);
        info.resource_report = "late replacement".into();
        req.body = serde_json::to_vec(&info).unwrap();
        assert_eq!(
            route(&req, &ready_flag(), &pool, &gh).await.unwrap(),
            CONFLICT
        );
        let json: String =
            sqlx::query_scalar("SELECT info_json FROM runner_metadata WHERE job_id = ?")
                .bind(job.id)
                .fetch_one(&pool)
                .await
                .unwrap();
        let saved: crate::criterion_report::RunnerInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(saved.resource_report, "baseline usage");
        server.abort();
    }

    #[tokio::test]
    async fn headers_keep_the_existing_128_kib_limit() {
        use tokio::io::AsyncWriteExt;
        for (length, allowed) in [(17 << 10, true), (129 << 10, false)] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                read_request(&mut stream).await
            });
            let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
            let request = format!(
                "GET /healthz HTTP/1.1\r\nX-Test: {}\r\n\r\n",
                "x".repeat(length)
            );
            let _ = stream.write_all(request.as_bytes()).await;
            let result = server.await.unwrap();
            if allowed {
                assert!(result.is_ok());
            } else {
                assert!(matches!(result, Err(RequestError::TooLarge)));
            }
        }
    }

    #[test]
    fn result_path_is_narrow() {
        assert_eq!(parse_job_result_path("/jobs/2/result"), Some(2));
        assert_eq!(parse_job_result_path("/jobs/2/result/more"), None);
        assert_eq!(parse_job_result_path("/jobs/no/result"), None);
    }

    #[test]
    fn runner_comments_rejected_for_terminal_states() {
        assert!(!status_allows_runner_comment("completed"));
        assert!(!status_allows_runner_comment("failed"));
    }
}
