use axum::{
    Json, Router,
    extract::{OriginalUri, Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::get,
};
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, DbErr, QueryResult, Statement};
use serde::{Deserialize, Serialize};

const DEFAULT_LIMIT: u64 = 50;
const MAX_LIMIT: u64 = 500;

#[derive(Debug, Clone)]
pub struct DashboardOptions {
    pub default_limit: u64,
    pub max_limit: u64,
}

impl Default for DashboardOptions {
    fn default() -> Self {
        Self {
            default_limit: DEFAULT_LIMIT,
            max_limit: MAX_LIMIT,
        }
    }
}

#[derive(Clone)]
struct DashboardState {
    db: DatabaseConnection,
    options: DashboardOptions,
}

#[derive(Debug, Clone, Deserialize)]
pub struct JobsQuery {
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobsResponse {
    pub jobs: Vec<DashboardJob>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DashboardJob {
    pub id: i64,
    pub job_type: String,
    pub status: String,
    pub payload: String,
    pub attempts: i32,
    pub max_attempts: i32,
    pub available_at: i64,
    pub locked_at: Option<i64>,
    pub last_error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub completed_at: Option<i64>,
    pub first_enqueued_at: Option<i64>,
    pub last_enqueued_at: Option<i64>,
    pub first_started_at: Option<i64>,
    pub last_started_at: Option<i64>,
    pub last_finished_at: Option<i64>,
    pub queued_ms_total: i64,
    pub queued_ms_last: Option<i64>,
    pub processing_ms_total: i64,
    pub processing_ms_last: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DashboardStats {
    pub total: i64,
    pub queued: i64,
    pub processing: i64,
    pub completed: i64,
    pub failed: i64,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Debug, thiserror::Error)]
enum DashboardError {
    #[error("database error: {0}")]
    Database(#[from] DbErr),
    #[error("row decode error: {0}")]
    RowDecode(String),
}

impl IntoResponse for DashboardError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: self.to_string(),
            }),
        )
            .into_response()
    }
}

type DashboardResult<T> = Result<T, DashboardError>;

pub fn router(db: DatabaseConnection) -> Router {
    router_with_options(db, DashboardOptions::default())
}

pub fn router_with_options(db: DatabaseConnection, options: DashboardOptions) -> Router {
    let state = DashboardState { db, options };

    Router::new()
        .route("/", get(index))
        .route("/api/stats", get(stats))
        .route("/api/jobs", get(jobs))
        .with_state(state)
}

async fn index(
    State(state): State<DashboardState>,
    OriginalUri(uri): OriginalUri,
) -> DashboardResult<Html<String>> {
    let limit = resolve_limit(None, &state.options);
    let stats = fetch_stats(&state.db).await?;
    let jobs = fetch_jobs(&state.db, limit).await?;
    let stats_path = api_path(uri.path(), "stats");
    let jobs_path = api_path(uri.path(), "jobs");

    Ok(Html(render_dashboard_html(
        &stats,
        &jobs,
        &stats_path,
        &jobs_path,
    )))
}

async fn stats(State(state): State<DashboardState>) -> DashboardResult<Json<DashboardStats>> {
    let stats = fetch_stats(&state.db).await?;
    Ok(Json(stats))
}

async fn jobs(
    State(state): State<DashboardState>,
    Query(query): Query<JobsQuery>,
) -> DashboardResult<Json<JobsResponse>> {
    let limit = resolve_limit(query.limit, &state.options);
    let jobs = fetch_jobs(&state.db, limit).await?;
    Ok(Json(JobsResponse { jobs }))
}

async fn fetch_stats(db: &DatabaseConnection) -> DashboardResult<DashboardStats> {
    let statement = Statement::from_string(
        DbBackend::Sqlite,
        "SELECT
             COUNT(*) AS total,
             COALESCE(SUM(CASE WHEN status = 'queued' THEN 1 ELSE 0 END), 0) AS queued,
             COALESCE(SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END), 0) AS processing,
             COALESCE(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END), 0) AS completed,
             COALESCE(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0) AS failed
         FROM jobs"
            .to_string(),
    );

    let row = db
        .query_one(statement)
        .await?
        .ok_or_else(|| DashboardError::RowDecode("no row returned".to_string()))?;

    Ok(DashboardStats {
        total: try_get_by_index::<i64>(&row, 0)?,
        queued: try_get_by_index::<i64>(&row, 1)?,
        processing: try_get_by_index::<i64>(&row, 2)?,
        completed: try_get_by_index::<i64>(&row, 3)?,
        failed: try_get_by_index::<i64>(&row, 4)?,
    })
}

async fn fetch_jobs(db: &DatabaseConnection, limit: i64) -> DashboardResult<Vec<DashboardJob>> {
    let statement = Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "SELECT
             id,
             job_type,
             status,
             payload,
             attempts,
             max_attempts,
             available_at,
             locked_at,
             last_error,
             created_at,
             updated_at,
             completed_at,
             first_enqueued_at,
             last_enqueued_at,
             first_started_at,
             last_started_at,
             last_finished_at,
             COALESCE(queued_ms_total, 0) AS queued_ms_total,
             queued_ms_last,
             COALESCE(processing_ms_total, 0) AS processing_ms_total,
             processing_ms_last
         FROM jobs
         ORDER BY id DESC
         LIMIT ?"
            .to_string(),
        vec![limit.into()],
    );

    let rows = db.query_all(statement).await?;
    let mut jobs = Vec::with_capacity(rows.len());

    for row in rows {
        jobs.push(DashboardJob {
            id: try_get_by_index::<i64>(&row, 0)?,
            job_type: try_get_by_index::<String>(&row, 1)?,
            status: try_get_by_index::<String>(&row, 2)?,
            payload: try_get_by_index::<String>(&row, 3)?,
            attempts: try_get_by_index::<i32>(&row, 4)?,
            max_attempts: try_get_by_index::<i32>(&row, 5)?,
            available_at: try_get_by_index::<i64>(&row, 6)?,
            locked_at: try_get_by_index::<Option<i64>>(&row, 7)?,
            last_error: try_get_by_index::<Option<String>>(&row, 8)?,
            created_at: try_get_by_index::<i64>(&row, 9)?,
            updated_at: try_get_by_index::<i64>(&row, 10)?,
            completed_at: try_get_by_index::<Option<i64>>(&row, 11)?,
            first_enqueued_at: try_get_by_index::<Option<i64>>(&row, 12)?,
            last_enqueued_at: try_get_by_index::<Option<i64>>(&row, 13)?,
            first_started_at: try_get_by_index::<Option<i64>>(&row, 14)?,
            last_started_at: try_get_by_index::<Option<i64>>(&row, 15)?,
            last_finished_at: try_get_by_index::<Option<i64>>(&row, 16)?,
            queued_ms_total: try_get_by_index::<i64>(&row, 17)?,
            queued_ms_last: try_get_by_index::<Option<i64>>(&row, 18)?,
            processing_ms_total: try_get_by_index::<i64>(&row, 19)?,
            processing_ms_last: try_get_by_index::<Option<i64>>(&row, 20)?,
        });
    }

    Ok(jobs)
}

fn resolve_limit(limit: Option<u64>, options: &DashboardOptions) -> i64 {
    let selected = limit.unwrap_or(options.default_limit).max(1);
    let clamped = selected.min(options.max_limit.max(1));
    i64::try_from(clamped).unwrap_or(i64::MAX)
}

fn api_path(index_path: &str, endpoint: &str) -> String {
    let mount = index_path.trim_end_matches('/');
    if mount.is_empty() {
        format!("/api/{endpoint}")
    } else {
        format!("{mount}/api/{endpoint}")
    }
}

fn try_get_by_index<T>(row: &QueryResult, index: usize) -> DashboardResult<T>
where
    T: sea_orm::TryGetable,
{
    row.try_get_by_index(index)
        .map_err(|e| DashboardError::RowDecode(format!("{e:?}")))
}

fn render_dashboard_html(
    stats: &DashboardStats,
    jobs: &[DashboardJob],
    stats_path: &str,
    jobs_path: &str,
) -> String {
    let mut rows = String::new();
    for job in jobs {
        let job_type = html_escape(&job.job_type);
        let status = html_escape(&job.status);
        let payload = html_escape(&truncate(&job.payload, 120));
        let last_error = html_escape(job.last_error.as_deref().unwrap_or(""));
        let queued = html_escape(&format_timing_ms(job.queued_ms_total, job.queued_ms_last));
        let processed = html_escape(&format_timing_ms(
            job.processing_ms_total,
            job.processing_ms_last,
        ));

        rows.push_str(&format!(
            "<tr>\
                <td>{}</td>\
                <td>{}</td>\
                <td>{}</td>\
                <td>{}/{}</td>\
                <td>{}</td>\
                <td>{}</td>\
                <td><code>{}</code></td>\
                <td><code>{}</code></td>\
             </tr>",
            job.id,
            job_type,
            status,
            job.attempts,
            job.max_attempts,
            queued,
            processed,
            payload,
            last_error
        ));
    }

    format!(
        "<!doctype html>\
         <html>\
         <head>\
           <meta charset='utf-8'>\
           <meta name='viewport' content='width=device-width, initial-scale=1'>\
           <title>lilqueue dashboard</title>\
           <style>\
             body {{ font-family: ui-sans-serif, system-ui, -apple-system, sans-serif; margin: 2rem; background: #111; color: #e5e5e5; }}\
             .stats {{ display: grid; gap: 0.75rem; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); margin-bottom: 1.5rem; }}\
             .card {{ border: 1px solid #3a3a3a; padding: 0.75rem; background: #1a1a1a; }}\
             .label {{ color: #a3a3a3; font-size: 0.8rem; }}\
             .value {{ font-size: 1.25rem; font-weight: 600; }}\
             table {{ border-collapse: collapse; width: 100%; }}\
             th, td {{ text-align: left; border-bottom: 1px solid #3a3a3a; padding: 0.5rem; vertical-align: top; }}\
             th {{ color: #bdbdbd; }}\
             code {{ white-space: pre-wrap; word-break: break-word; font-size: 0.85rem; color: #f5f5f5; }}\
             a {{ color: #93c5fd; }}\
             .links {{ margin-bottom: 1rem; }}\
           </style>\
         </head>\
         <body>\
           <h1>lilqueue dashboard</h1>\
           <p class='links'>JSON: <a href='{}'>{}</a> | <a href='{}'>{}</a></p>\
           <section class='stats'>\
             <div class='card'><div class='label'>total</div><div class='value'>{}</div></div>\
             <div class='card'><div class='label'>queued</div><div class='value'>{}</div></div>\
             <div class='card'><div class='label'>processing</div><div class='value'>{}</div></div>\
             <div class='card'><div class='label'>completed</div><div class='value'>{}</div></div>\
             <div class='card'><div class='label'>failed</div><div class='value'>{}</div></div>\
           </section>\
           <table>\
             <thead>\
               <tr><th>ID</th><th>Type</th><th>Status</th><th>Attempts</th><th>Queued</th><th>Processed</th><th>Payload</th><th>Last error</th></tr>\
             </thead>\
             <tbody>{}</tbody>\
           </table>\
         </body>\
         </html>",
        stats_path,
        stats_path,
        jobs_path,
        jobs_path,
        stats.total,
        stats.queued,
        stats.processing,
        stats.completed,
        stats.failed,
        rows
    )
}

fn truncate(input: &str, max_chars: usize) -> String {
    let mut chars = input.chars();
    let mut result = String::new();

    for _ in 0..max_chars {
        let Some(ch) = chars.next() else {
            return result;
        };
        result.push(ch);
    }

    if chars.next().is_some() {
        result.push_str("...");
    }

    result
}

fn html_escape(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn format_timing_ms(total: i64, last: Option<i64>) -> String {
    let last_display = last.unwrap_or_default();
    format!("{total} ms (last {last_display} ms)")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Job, JobError, ProcessorOptions, SqliteJobProcessor};
    use async_trait::async_trait;
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
        Router,
    };
    use std::{path::Path, time::Duration};
    use tempfile::tempdir;
    use tower::util::ServiceExt;

    #[derive(Debug, Serialize, Deserialize)]
    struct DashboardTestJob {
        value: String,
    }

    #[async_trait]
    impl Job for DashboardTestJob {
        async fn process(&self) -> Result<(), JobError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn dashboard_routes_return_stats_and_jobs() {
        let dir = tempdir().unwrap();
        let db_url = sqlite_url(dir.path().join("queue.db"));

        let processor = SqliteJobProcessor::<DashboardTestJob>::connect(&db_url, test_options())
            .await
            .unwrap();

        processor
            .enqueue(&DashboardTestJob {
                value: "first".to_string(),
            })
            .await
            .unwrap();
        processor
            .enqueue(&DashboardTestJob {
                value: "second".to_string(),
            })
            .await
            .unwrap();

        let app = router(processor.db().clone());

        let stats_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(stats_response.status(), StatusCode::OK);

        let stats_bytes = to_bytes(stats_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let stats: DashboardStats = serde_json::from_slice(&stats_bytes).unwrap();
        assert_eq!(stats.total, 2);
        assert_eq!(stats.queued, 2);
        assert_eq!(stats.processing, 0);

        let jobs_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/jobs?limit=1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(jobs_response.status(), StatusCode::OK);

        let jobs_bytes = to_bytes(jobs_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let jobs: JobsResponse = serde_json::from_slice(&jobs_bytes).unwrap();
        assert_eq!(jobs.jobs.len(), 1);
        assert!(jobs.jobs[0].first_enqueued_at.is_some());
        assert!(jobs.jobs[0].last_enqueued_at.is_some());
        assert!(jobs.jobs[0].queued_ms_total >= 0);
        assert!(jobs.jobs[0].processing_ms_total >= 0);

        let index_response = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(index_response.status(), StatusCode::OK);

        let index_bytes = to_bytes(index_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let html = String::from_utf8(index_bytes.to_vec()).unwrap();
        assert!(html.contains("lilqueue dashboard"));
        assert!(html.contains("/api/jobs"));
        assert!(html.contains("<th>Queued</th>"));
        assert!(html.contains("<th>Processed</th>"));
    }

    #[tokio::test]
    async fn dashboard_stats_are_zero_for_empty_queue() {
        let dir = tempdir().unwrap();
        let db_url = sqlite_url(dir.path().join("queue.db"));

        let processor = SqliteJobProcessor::<DashboardTestJob>::connect(&db_url, test_options())
            .await
            .unwrap();

        let app = router(processor.db().clone());

        let stats_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(stats_response.status(), StatusCode::OK);

        let stats_bytes = to_bytes(stats_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let stats: DashboardStats = serde_json::from_slice(&stats_bytes).unwrap();
        assert_eq!(stats.total, 0);
        assert_eq!(stats.queued, 0);
        assert_eq!(stats.processing, 0);
        assert_eq!(stats.completed, 0);
        assert_eq!(stats.failed, 0);
    }

    #[tokio::test]
    async fn dashboard_index_links_respect_mount_path() {
        let dir = tempdir().unwrap();
        let db_url = sqlite_url(dir.path().join("queue.db"));

        let processor = SqliteJobProcessor::<DashboardTestJob>::connect(&db_url, test_options())
            .await
            .unwrap();
        processor
            .enqueue(&DashboardTestJob {
                value: "first".to_string(),
            })
            .await
            .unwrap();

        let app = Router::new().nest("/queue", router(processor.db().clone()));

        let index_response = app
            .oneshot(Request::builder().uri("/queue").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(index_response.status(), StatusCode::OK);

        let index_bytes = to_bytes(index_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let html = String::from_utf8(index_bytes.to_vec()).unwrap();
        assert!(html.contains("href='/queue/api/stats'"));
        assert!(html.contains("href='/queue/api/jobs'"));
    }

    fn test_options() -> ProcessorOptions {
        ProcessorOptions {
            max_attempts: 3,
            lock_timeout: Duration::from_secs(30),
            poll_interval: Duration::from_millis(1),
            backoff: crate::BackoffStrategy::Fixed(Duration::ZERO),
        }
    }

    fn sqlite_url(path: impl AsRef<Path>) -> String {
        format!("sqlite://{}?mode=rwc", path.as_ref().display())
    }
}
