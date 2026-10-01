//! Thin HTTP client for the GitHub REST API.
//!
//! Wraps [`reqwest::Client`] with authentication, standard headers, and
//! response-status checking. Retries transient errors with exponential backoff.

use anyhow::{Context, Result};
use backon::{ExponentialBuilder, Retryable};
use reqwest::header::{ACCEPT, USER_AGENT};
use reqwest::{Client, RequestBuilder, Response, StatusCode};

use crate::models::GitHubComment;

const API_BASE: &str = "https://api.github.com";

fn normalize_ref(reference: &str) -> &str {
    reference.strip_prefix("origin/").unwrap_or(reference)
}

#[derive(serde::Deserialize)]
struct CommitIdentity {
    sha: String,
}

/// Append the benchmark runner issues link if configured.
pub fn issues_footer(runner_repo_url: Option<&str>) -> String {
    match runner_repo_url {
        Some(url) if !url.is_empty() => {
            format!("\n\n---\n[File an issue]({url}/issues) against this benchmark runner")
        }
        _ => String::new(),
    }
}

/// Maximum number of pages to fetch when paginating (10,000 comments at 100/page).
const MAX_PAGES: usize = 100;

/// Thin HTTP client for the GitHub REST API.
#[derive(Clone)]
pub struct GitHubClient {
    client: Client,
    token: String,
    #[cfg(test)]
    api_base: Option<String>,
}

/// Determine whether an error (or status) is worth retrying.
pub(crate) fn is_retryable(err: &anyhow::Error) -> bool {
    // Check for reqwest errors (network / connection failures)
    if let Some(re) = err.downcast_ref::<reqwest::Error>() {
        if re.is_connect() || re.is_timeout() || re.is_request() {
            return true;
        }
        if let Some(status) = re.status() {
            return status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS;
        }
        return true; // unknown reqwest errors → retry
    }

    // Check for our own "GitHub API … error …" messages (from check_response)
    let msg = err.to_string();
    if msg.contains("error 5") || msg.contains("error 429") {
        return true;
    }
    false
}

/// Parse the `Retry-After` header (seconds) from a 429 response.
fn parse_retry_after(resp: &Response) -> Option<u64> {
    resp.headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
}

/// Parse the `next` URL from a `Link` header value.
fn parse_next_link(link_header: &str) -> Option<String> {
    for part in link_header.split(',') {
        let part = part.trim();
        if part.ends_with("rel=\"next\"") {
            if let Some(url) = part.strip_suffix(">; rel=\"next\"") {
                if let Some(url) = url.strip_prefix('<') {
                    return Some(url.to_string());
                }
            }
        }
    }
    None
}

impl GitHubClient {
    /// Create a new client with the given personal access token.
    pub fn new(token: &str) -> Self {
        Self {
            client: Client::new(),
            token: token.to_string(),
            #[cfg(test)]
            api_base: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn test_client(api_base: String) -> Self {
        Self {
            api_base: Some(api_base),
            ..Self::new("test-token")
        }
    }

    /// Attach standard GitHub API headers and bearer auth to a request.
    fn request_builder(&self, builder: RequestBuilder) -> RequestBuilder {
        builder
            .bearer_auth(&self.token)
            .header(ACCEPT, "application/vnd.github+json")
            .header(USER_AGENT, "datafusion-benchmark-controller")
    }

    /// Check that a response has a 2xx status, returning an error with the
    /// response body on failure. For 429 responses, sleeps for `Retry-After`
    /// before returning the error (so backon's retry fires after the wait).
    async fn check_response(resp: Response, context: &str) -> Result<Response> {
        if resp.status().is_success() {
            return Ok(resp);
        }
        let status = resp.status();

        // Respect Retry-After on 429
        if status == StatusCode::TOO_MANY_REQUESTS {
            if let Some(secs) = parse_retry_after(&resp) {
                tracing::warn!(retry_after = secs, "rate limited, sleeping");
                tokio::time::sleep(tokio::time::Duration::from_secs(secs)).await;
            }
        }

        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("GitHub API {context} error {status}: {body}");
    }

    /// Send a GET request with retry logic. Returns the successful response.
    async fn get_with_retry(&self, url: &str, query: &[(&str, &str)]) -> Result<Response> {
        let url = url.to_string();
        #[cfg(test)]
        let url = match (&self.api_base, url.strip_prefix(API_BASE)) {
            (Some(base), Some(path)) => format!("{base}{path}"),
            _ => url,
        };
        let query: Vec<(String, String)> = query
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        (|| {
            let url = url.clone();
            let query = query.clone();
            async move {
                let resp = self
                    .request_builder(self.client.get(&url))
                    .query(&query)
                    .send()
                    .await
                    .context("send request")?;
                Self::check_response(resp, "GET").await
            }
        })
        .retry(ExponentialBuilder::default().with_max_times(3))
        .sleep(tokio::time::sleep)
        .when(is_retryable)
        .await
    }

    /// Send a POST request with retry logic. Returns the successful response.
    async fn post_with_retry(&self, url: &str, body: serde_json::Value) -> Result<Response> {
        let url = url.to_string();
        #[cfg(test)]
        let url = match (&self.api_base, url.strip_prefix(API_BASE)) {
            (Some(base), Some(path)) => format!("{base}{path}"),
            _ => url,
        };

        (|| {
            let url = url.clone();
            let body = body.clone();
            async move {
                let resp = self
                    .request_builder(self.client.post(&url))
                    .json(&body)
                    .send()
                    .await
                    .context("send request")?;
                Self::check_response(resp, "POST").await
            }
        })
        .retry(ExponentialBuilder::default().with_max_times(3))
        .sleep(tokio::time::sleep)
        .when(is_retryable)
        .await
    }

    /// Fetch issue/PR comments updated since `since` (ISO 8601), paginating through all results.
    /// Caps at MAX_PAGES pages (10,000 comments).
    #[tracing::instrument(skip(self, since))]
    pub async fn fetch_recent_comments(
        &self,
        repo: &str,
        since: &str,
    ) -> Result<Vec<GitHubComment>> {
        let mut all_comments = Vec::new();
        let mut next_url: Option<String> = None;

        for page in 0..MAX_PAGES {
            let resp = if page == 0 {
                let url = format!("{API_BASE}/repos/{repo}/issues/comments");
                self.get_with_retry(
                    &url,
                    &[
                        ("per_page", "100"),
                        ("sort", "updated"),
                        ("direction", "desc"),
                        ("since", since),
                    ],
                )
                .await
                .context("fetch comments")?
            } else {
                let url = next_url.as_deref().unwrap();
                self.get_with_retry(url, &[])
                    .await
                    .context("fetch comments page")?
            };

            // Parse next link before consuming the body
            let link_header = resp
                .headers()
                .get(reqwest::header::LINK)
                .and_then(|v| v.to_str().ok())
                .and_then(parse_next_link);

            let comments: Vec<GitHubComment> = resp.json().await.context("parse comments")?;
            let count = comments.len();
            all_comments.extend(comments);

            match link_header {
                Some(url) if count > 0 => next_url = Some(url),
                _ => break,
            }
        }

        Ok(all_comments)
    }

    /// Post a comment on a PR/issue.
    #[tracing::instrument(skip(self, body))]
    pub async fn post_comment(&self, repo: &str, pr_number: i64, body: &str) -> Result<()> {
        let url = format!("{API_BASE}/repos/{repo}/issues/{pr_number}/comments");
        self.post_with_retry(&url, serde_json::json!({ "body": body }))
            .await
            .context("post comment")?;
        Ok(())
    }

    /// Publish a controller-owned notification once. The reconciliation loop is
    /// the sole caller. A marker lets the next pass recover when GitHub accepted
    /// a POST but the response or subsequent SQLite write was lost.
    ///
    /// Unlike ordinary comments, never retry this POST blindly: look it up first
    /// on the next reconciliation pass instead.
    pub async fn ensure_run_comment(
        &self,
        repo: &str,
        pr_number: i64,
        marker: &str,
        body: &str,
    ) -> Result<i64> {
        let url = format!("{API_BASE}/repos/{repo}/issues/{pr_number}/comments");
        for page in 1..=MAX_PAGES {
            let comments: Vec<GitHubComment> = self
                .get_with_retry(&url, &[("per_page", "100"), ("page", &page.to_string())])
                .await?
                .json()
                .await?;
            if let Some(comment) = comments.iter().find(|c| c.body_text().ends_with(marker)) {
                return Ok(comment.id);
            }
            if comments.len() < 100 {
                #[cfg(test)]
                let url = match &self.api_base {
                    Some(base) => url.replacen(API_BASE, base, 1),
                    None => url.clone(),
                };
                let response = self
                    .request_builder(self.client.post(&url))
                    .json(&serde_json::json!({"body": format!("{body}\n\n{marker}")}))
                    .send()
                    .await?;
                let comment: GitHubComment = Self::check_response(response, "POST notification")
                    .await?
                    .json()
                    .await?;
                return Ok(comment.id);
            }
        }
        anyhow::bail!("comment pagination limit reached; refusing to risk a duplicate notification")
    }

    /// Recover or create an unlisted comparison Gist. As with notifications,
    /// never retry the POST blindly: a lost response may hide a successful write.
    /// Listing the authenticated user's Gists also prevents other users from
    /// spoofing an execution's recovery key.
    pub async fn ensure_comparison_gist(
        &self,
        key: &str,
        description: &str,
        comparison: &str,
    ) -> Result<String> {
        #[derive(serde::Deserialize)]
        struct Gist {
            description: Option<String>,
            html_url: String,
        }
        let marker = format!("[benchmark-comparison:{key}]");
        let url = format!("{API_BASE}/gists");
        for page in 1..=MAX_PAGES {
            let gists: Vec<Gist> = self
                .get_with_retry(&url, &[("per_page", "100"), ("page", &page.to_string())])
                .await?
                .json()
                .await?;
            if let Some(gist) = gists.iter().find(|g| {
                g.description
                    .as_deref()
                    .is_some_and(|d| d.ends_with(&marker))
            }) {
                return Ok(gist.html_url.clone());
            }
            if gists.len() < 100 {
                #[cfg(test)]
                let url = match &self.api_base {
                    Some(base) => url.replacen(API_BASE, base, 1),
                    None => url.clone(),
                };
                let response = self
                    .request_builder(self.client.post(&url))
                    .json(&serde_json::json!({
                        "description": format!("{description} {marker}"),
                        "public": false,
                        "files": { "comparison.txt": { "content": comparison } }
                    }))
                    .send()
                    .await?;
                let gist: Gist = Self::check_response(response, "POST comparison Gist")
                    .await?
                    .json()
                    .await?;
                return Ok(gist.html_url);
            }
        }
        anyhow::bail!("Gist pagination limit reached; refusing to risk a duplicate comparison")
    }

    /// Look up a PR and return its `head.ref` (the source branch name).
    /// Runner pods no longer have a `GITHUB_TOKEN`, so the controller
    /// resolves this once and passes it to the pod via `PR_HEAD_REF`.
    #[tracing::instrument(skip(self))]
    pub async fn get_pr_head_ref(&self, repo: &str, pr_number: i64) -> Result<String> {
        #[derive(serde::Deserialize)]
        struct PullHead {
            #[serde(rename = "ref")]
            ref_: String,
        }
        #[derive(serde::Deserialize)]
        struct Pull {
            head: PullHead,
        }
        let url = format!("{API_BASE}/repos/{repo}/pulls/{pr_number}");
        let resp = self.get_with_retry(&url, &[]).await?;
        let pull: Pull = resp.json().await.context("parse pull json")?;
        Ok(pull.head.ref_)
    }

    /// Resolve immutable source identities once, before independent shards are
    /// queued. No clone, build or benchmark inventory is needed here.
    pub async fn freeze_sources(
        &self,
        repo: &str,
        pr_number: i64,
        baseline_ref: Option<&str>,
        changed_ref: Option<&str>,
    ) -> Result<crate::sharding::FrozenSources> {
        #[derive(serde::Deserialize)]
        struct Head {
            sha: String,
            #[serde(rename = "ref")]
            name: String,
        }
        #[derive(serde::Deserialize)]
        struct Pull {
            head: Head,
        }
        #[derive(serde::Deserialize)]
        struct Comparison {
            merge_base_commit: CommitIdentity,
        }

        let pull: Pull = self
            .get_with_retry(&format!("{API_BASE}/repos/{repo}/pulls/{pr_number}"), &[])
            .await?
            .json()
            .await
            .context("parse PR source identity")?;
        let changed_sha = match changed_ref {
            None => pull.head.sha.clone(),
            Some(name) if name == pull.head.name && name != "main" => pull.head.sha.clone(),
            Some(name) => self.resolve_commit(repo, name).await?,
        };
        let baseline_sha = match baseline_ref {
            // Identical refs in A/A requests must not be resolved twice while
            // a branch can move between API calls.
            Some(name) if changed_ref.map(normalize_ref) == Some(normalize_ref(name)) => {
                changed_sha.clone()
            }
            // Explicit baseline `main` means upstream main, even for a fork
            // PR whose source branch happens to be named main as well.
            Some(name) if name == pull.head.name && name != "main" => pull.head.sha.clone(),
            Some(name) => self.resolve_commit(repo, name).await?,
            None => {
                // Preserve the runner's existing default: merge-base(PR head,
                // main), even when the changed side has a custom override.
                let main = if changed_ref.map(normalize_ref) == Some("main") {
                    changed_sha.clone()
                } else {
                    self.resolve_commit(repo, "main").await?
                };
                let url = format!("{API_BASE}/repos/{repo}/compare/{main}...{}", pull.head.sha);
                let comparison: Comparison = self
                    .get_with_retry(&url, &[("per_page", "1")])
                    .await?
                    .json()
                    .await
                    .context("parse merge-base")?;
                comparison.merge_base_commit.sha
            }
        };
        let sources = crate::sharding::FrozenSources {
            baseline_sha,
            changed_sha,
            pr_head_ref: pull.head.name,
        };
        sources.validate()?;
        Ok(sources)
    }

    async fn resolve_commit(&self, repo: &str, reference: &str) -> Result<String> {
        let reference = normalize_ref(reference);
        let mut url = reqwest::Url::parse(&format!("{API_BASE}/repos/{repo}/commits/"))?;
        url.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("invalid GitHub URL"))?
            .pop_if_empty()
            .push(reference);
        let commit: CommitIdentity = self
            .get_with_retry(url.as_str(), &[])
            .await?
            .json()
            .await
            .with_context(|| format!("resolve commit {reference}"))?;
        Ok(commit.sha)
    }

    /// Add a reaction (e.g. "rocket") to a comment. Logs a warning on failure instead of erroring.
    pub async fn post_reaction(&self, repo: &str, comment_id: i64, content: &str) -> Result<()> {
        let url = format!("{API_BASE}/repos/{repo}/issues/comments/{comment_id}/reactions");
        let body = serde_json::json!({ "content": content });

        match self.post_with_retry(&url, body).await {
            Ok(_) => Ok(()),
            Err(e) => {
                tracing::warn!(error = %e, "failed to post reaction");
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn mock_gets(
        responses: Vec<(String, serde_json::Value)>,
    ) -> (GitHubClient, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = GitHubClient::new("test-token");
        client.api_base = Some(format!("http://{}", listener.local_addr().unwrap()));
        let server = tokio::spawn(async move {
            for (path, body) in responses {
                let (mut stream, _) =
                    tokio::time::timeout(std::time::Duration::from_secs(10), listener.accept())
                        .await
                        .unwrap()
                        .unwrap();
                let mut request = Vec::new();
                loop {
                    let mut chunk = [0; 2048];
                    let n = stream.read(&mut chunk).await.unwrap();
                    assert!(n > 0);
                    request.extend_from_slice(&chunk[..n]);
                    if request.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                    assert!(request.len() < 16384);
                }
                assert!(String::from_utf8_lossy(&request)
                    .starts_with(&format!("GET {path} HTTP/1.1\r\n")));
                let body = body.to_string();
                stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            }
        });
        (client, server)
    }

    #[tokio::test]
    async fn freezing_uses_one_pr_snapshot_and_immutable_merge_base_inputs() {
        let head = "b".repeat(40);
        let main = "c".repeat(40);
        let base = "a".repeat(40);
        let changed = "d".repeat(40);
        let (client, server) = mock_gets(vec![
            (
                "/repos/test/repo/pulls/9".into(),
                serde_json::json!({"head":{"sha":head,"ref":"fork-feature"}}),
            ),
            (
                "/repos/test/repo/commits/v1%2Ftag".into(),
                serde_json::json!({"sha":changed}),
            ),
            (
                "/repos/test/repo/commits/main".into(),
                serde_json::json!({"sha":main}),
            ),
            (
                format!("/repos/test/repo/compare/{main}...{head}?per_page=1"),
                serde_json::json!({"merge_base_commit":{"sha":base}}),
            ),
        ])
        .await;
        let sources = client
            .freeze_sources("test/repo", 9, None, Some("v1/tag"))
            .await
            .unwrap();
        assert_eq!(sources.baseline_sha, base);
        assert_eq!(sources.changed_sha, changed);
        assert_eq!(sources.pr_head_ref, "fork-feature");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn explicit_main_baseline_is_not_a_forks_main_branch() {
        let head = "b".repeat(40);
        let main = "c".repeat(40);
        let (client, server) = mock_gets(vec![
            (
                "/repos/test/repo/pulls/9".into(),
                serde_json::json!({"head":{"sha":head,"ref":"main"}}),
            ),
            (
                "/repos/test/repo/commits/main".into(),
                serde_json::json!({"sha":main}),
            ),
        ])
        .await;
        let sources = client
            .freeze_sources("test/repo", 9, Some("main"), None)
            .await
            .unwrap();
        assert_eq!(sources.baseline_sha, main);
        assert_eq!(sources.changed_sha, head);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn aa_branch_refs_are_resolved_once() {
        let main = "c".repeat(40);
        let (client, server) = mock_gets(vec![
            (
                "/repos/test/repo/pulls/9".into(),
                serde_json::json!({"head":{"sha":"b".repeat(40),"ref":"main"}}),
            ),
            (
                "/repos/test/repo/commits/main".into(),
                serde_json::json!({"sha":main}),
            ),
        ])
        .await;
        let sources = client
            .freeze_sources("test/repo", 9, Some("origin/main"), Some("main"))
            .await
            .unwrap();
        assert_eq!(sources.baseline_sha, main);
        assert_eq!(sources.changed_sha, main);
        server.await.unwrap();
    }

    #[test]
    fn parse_next_link_standard() {
        let header = r#"<https://api.github.com/repos/foo/bar/issues/comments?page=2>; rel="next", <https://api.github.com/repos/foo/bar/issues/comments?page=5>; rel="last""#;
        assert_eq!(
            parse_next_link(header),
            Some("https://api.github.com/repos/foo/bar/issues/comments?page=2".to_string())
        );
    }

    #[test]
    fn parse_next_link_missing() {
        let header = r#"<https://api.github.com/repos/foo/bar/issues/comments?page=5>; rel="last""#;
        assert_eq!(parse_next_link(header), None);
    }

    #[test]
    fn parse_next_link_empty() {
        assert_eq!(parse_next_link(""), None);
    }

    #[test]
    fn retryable_on_server_error_message() {
        let err = anyhow::anyhow!("GitHub API GET error 500 Internal Server Error: oops");
        assert!(is_retryable(&err));
    }

    #[test]
    fn retryable_on_429_message() {
        let err = anyhow::anyhow!("GitHub API GET error 429 Too Many Requests: slow down");
        assert!(is_retryable(&err));
    }

    #[test]
    fn not_retryable_on_404() {
        let err = anyhow::anyhow!("GitHub API GET error 404 Not Found: nope");
        assert!(!is_retryable(&err));
    }

    #[test]
    fn not_retryable_on_401() {
        let err = anyhow::anyhow!("GitHub API GET error 401 Unauthorized: bad token");
        assert!(!is_retryable(&err));
    }
}
