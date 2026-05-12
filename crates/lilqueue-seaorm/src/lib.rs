use async_trait::async_trait;
use lilqueue::{
    BoxError, ClaimedJob, JobQueue, LockableQueue, NewJob, QueueResult, RetryableQueue,
    dashboard::{DashboardData, DashboardJob, DashboardStats},
};
use sea_orm::{ConnectionTrait, Database, DatabaseConnection, DbBackend, DbErr, Statement};
use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const STATUS_QUEUED: &str = "queued";
const STATUS_PROCESSING: &str = "processing";
const STATUS_COMPLETED: &str = "completed";
const STATUS_FAILED: &str = "failed";

#[derive(Debug, Clone)]
pub struct SeaOrmQueueOptions {
    pub lock_timeout: Duration,
}

impl Default for SeaOrmQueueOptions {
    fn default() -> Self {
        Self {
            lock_timeout: Duration::from_secs(300),
        }
    }
}

#[derive(Clone)]
pub struct SeaOrmQueue {
    db: DatabaseConnection,
    options: SeaOrmQueueOptions,
    worker_id: String,
    claim_counter: Arc<AtomicU64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeaOrmClaim {
    pub lock_token: String,
    pub started_at: i64,
}

impl SeaOrmQueue {
    pub async fn connect(database_url: &str, options: SeaOrmQueueOptions) -> Result<Self, DbErr> {
        let db = Database::connect(database_url).await?;
        Self::new(db, options).await
    }

    pub async fn new(db: DatabaseConnection, options: SeaOrmQueueOptions) -> Result<Self, DbErr> {
        let queue = Self {
            db,
            options,
            worker_id: make_worker_id(),
            claim_counter: Arc::new(AtomicU64::new(1)),
        };
        queue.initialize_schema().await?;
        Ok(queue)
    }

    pub fn db(&self) -> &DatabaseConnection {
        &self.db
    }

    pub fn options(&self) -> &SeaOrmQueueOptions {
        &self.options
    }

    async fn initialize_schema(&self) -> Result<(), DbErr> {
        self.db
            .execute(Statement::from_string(
                DbBackend::Sqlite,
                "CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL,
                    available_at INTEGER NOT NULL,
                    locked_at INTEGER NULL,
                    lock_token TEXT NULL,
                    last_error TEXT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    completed_at INTEGER NULL,
                    first_enqueued_at INTEGER NULL,
                    last_enqueued_at INTEGER NULL,
                    first_started_at INTEGER NULL,
                    last_started_at INTEGER NULL,
                    last_finished_at INTEGER NULL,
                    queued_ms_total INTEGER NOT NULL DEFAULT 0,
                    queued_ms_last INTEGER NULL,
                    processing_ms_total INTEGER NOT NULL DEFAULT 0,
                    processing_ms_last INTEGER NULL
                )"
                .to_string(),
            ))
            .await?;

        self.ensure_timing_columns().await?;

        self.db
            .execute(Statement::from_string(
                DbBackend::Sqlite,
                "CREATE INDEX IF NOT EXISTS idx_jobs_ready
                    ON jobs (job_type, status, available_at, id)"
                    .to_string(),
            ))
            .await?;

        self.db
            .execute(Statement::from_string(
                DbBackend::Sqlite,
                "CREATE INDEX IF NOT EXISTS idx_jobs_processing
                    ON jobs (job_type, status, locked_at)"
                    .to_string(),
            ))
            .await?;

        Ok(())
    }

    async fn ensure_timing_columns(&self) -> Result<(), DbErr> {
        let existing = self.job_columns().await?;
        for (column, definition) in timing_column_definitions() {
            if !existing.contains(column) {
                self.db
                    .execute(Statement::from_string(
                        DbBackend::Sqlite,
                        format!("ALTER TABLE jobs ADD COLUMN {column} {definition}"),
                    ))
                    .await?;
            }
        }

        self.db
            .execute(Statement::from_string(
                DbBackend::Sqlite,
                "UPDATE jobs
                 SET first_enqueued_at = COALESCE(first_enqueued_at, created_at)"
                    .to_string(),
            ))
            .await?;

        Ok(())
    }

    async fn job_columns(&self) -> Result<HashSet<String>, DbErr> {
        let rows = self
            .db
            .query_all(Statement::from_string(
                DbBackend::Sqlite,
                "PRAGMA table_info(jobs)".to_string(),
            ))
            .await?;

        let mut columns = HashSet::with_capacity(rows.len());
        for row in rows {
            columns.insert(row.try_get_by_index::<String>(1)?);
        }

        Ok(columns)
    }

    async fn reclaim_stale_locks(&self, job_type: &str, now: i64) -> Result<(), DbErr> {
        let stale_before = now.saturating_sub(duration_to_secs(self.options.lock_timeout));
        self.db
            .execute(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "UPDATE jobs
                 SET status = ?,
                     locked_at = NULL,
                     lock_token = NULL,
                     updated_at = ?,
                     last_enqueued_at = ?,
                     last_finished_at = ?,
                     processing_ms_last = CASE
                         WHEN ? >= COALESCE(last_started_at, locked_at, ?)
                         THEN (? - COALESCE(last_started_at, locked_at, ?)) * 1000
                         ELSE 0
                     END,
                     processing_ms_total = processing_ms_total + CASE
                         WHEN ? >= COALESCE(last_started_at, locked_at, ?)
                         THEN (? - COALESCE(last_started_at, locked_at, ?)) * 1000
                         ELSE 0
                     END
                 WHERE job_type = ?
                   AND status = ?
                   AND locked_at IS NOT NULL
                   AND locked_at <= ?"
                    .to_string(),
                vec![
                    STATUS_QUEUED.into(),
                    now.into(),
                    now.into(),
                    now.into(),
                    now.into(),
                    now.into(),
                    now.into(),
                    now.into(),
                    now.into(),
                    now.into(),
                    now.into(),
                    now.into(),
                    job_type.into(),
                    STATUS_PROCESSING.into(),
                    stale_before.into(),
                ],
            ))
            .await?;
        Ok(())
    }

    fn next_lock_token(&self, now: i64) -> String {
        let counter = self.claim_counter.fetch_add(1, Ordering::Relaxed);
        format!("{}-{}-{}", self.worker_id, now, counter)
    }
}

#[async_trait]
impl JobQueue for SeaOrmQueue {
    async fn enqueue(&self, job: NewJob) -> QueueResult<i64> {
        let row = self
            .db
            .query_one(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "INSERT INTO jobs
                 (job_type, payload, status, attempts, max_attempts, available_at, locked_at,
                  lock_token, last_error, created_at, updated_at, completed_at,
                  first_enqueued_at, last_enqueued_at, first_started_at, last_started_at,
                  last_finished_at, queued_ms_total, queued_ms_last, processing_ms_total,
                  processing_ms_last)
                 VALUES (?, ?, ?, 0, ?, ?, NULL, NULL, NULL, ?, ?, NULL, ?, ?, NULL, NULL,
                         NULL, 0, NULL, 0, NULL)
                 RETURNING id"
                    .to_string(),
                vec![
                    job.job_type.into(),
                    job.payload.into(),
                    STATUS_QUEUED.into(),
                    i64::from(job.max_attempts).into(),
                    job.available_at.into(),
                    job.enqueued_at.into(),
                    job.enqueued_at.into(),
                    job.enqueued_at.into(),
                    job.enqueued_at.into(),
                ],
            ))
            .await?
            .ok_or_else(|| std::io::Error::other("insert returned no row"))?;

        Ok(row.try_get_by_index::<i64>(0)?)
    }

    async fn next_wakeup_at(&self, job_type: &str) -> QueueResult<Option<i64>> {
        let lock_timeout_secs = duration_to_secs(self.options.lock_timeout);
        let row = self
            .db
            .query_one(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "SELECT MIN(
                    CASE
                        WHEN status = ? THEN available_at
                        WHEN status = ? AND locked_at IS NOT NULL THEN locked_at + ?
                        ELSE NULL
                    END
                 )
                 FROM jobs
                 WHERE job_type = ?
                   AND status IN (?, ?)"
                    .to_string(),
                vec![
                    STATUS_QUEUED.into(),
                    STATUS_PROCESSING.into(),
                    lock_timeout_secs.into(),
                    job_type.into(),
                    STATUS_QUEUED.into(),
                    STATUS_PROCESSING.into(),
                ],
            ))
            .await?;

        match row {
            Some(row) => Ok(row.try_get_by_index::<Option<i64>>(0)?),
            None => Ok(None),
        }
    }
}

#[async_trait]
impl LockableQueue for SeaOrmQueue {
    type Claim = SeaOrmClaim;

    async fn claim(&self, job_type: &str) -> QueueResult<Option<ClaimedJob<Self::Claim>>> {
        let now = now_epoch_seconds()?;
        self.reclaim_stale_locks(job_type, now).await?;

        let lock_token = self.next_lock_token(now);
        let row = self
            .db
            .query_one(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "UPDATE jobs
                 SET status = ?,
                     attempts = attempts + 1,
                     locked_at = ?,
                     lock_token = ?,
                     updated_at = ?,
                     queued_ms_last = CASE
                         WHEN ? >= COALESCE(last_enqueued_at, ?)
                         THEN (? - COALESCE(last_enqueued_at, ?)) * 1000
                         ELSE 0
                     END,
                     queued_ms_total = queued_ms_total + CASE
                         WHEN ? >= COALESCE(last_enqueued_at, ?)
                         THEN (? - COALESCE(last_enqueued_at, ?)) * 1000
                         ELSE 0
                     END,
                     first_started_at = COALESCE(first_started_at, ?),
                     last_started_at = ?
                 WHERE id = (
                     SELECT id
                     FROM jobs
                     WHERE job_type = ?
                       AND status = ?
                       AND available_at <= ?
                     ORDER BY available_at ASC, id ASC
                     LIMIT 1
                 )
                 AND status = ?
                 RETURNING id, payload, attempts, max_attempts, lock_token, last_started_at"
                    .to_string(),
                vec![
                    STATUS_PROCESSING.into(),
                    now.into(),
                    lock_token.clone().into(),
                    now.into(),
                    now.into(),
                    now.into(),
                    now.into(),
                    now.into(),
                    now.into(),
                    now.into(),
                    now.into(),
                    now.into(),
                    now.into(),
                    now.into(),
                    job_type.into(),
                    STATUS_QUEUED.into(),
                    now.into(),
                    STATUS_QUEUED.into(),
                ],
            ))
            .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let id = row.try_get_by_index::<i64>(0)?;
        let payload = row.try_get_by_index::<String>(1)?;
        let attempts = row.try_get_by_index::<i64>(2)?;
        let max_attempts = row.try_get_by_index::<i64>(3)?;
        let stored_lock_token = row.try_get_by_index::<Option<String>>(4)?;
        let started_at = row.try_get_by_index::<Option<i64>>(5)?;

        Ok(Some(ClaimedJob {
            id,
            payload,
            attempts: u32::try_from(attempts)?,
            max_attempts: u32::try_from(max_attempts)?,
            claim: SeaOrmClaim {
                lock_token: stored_lock_token.unwrap_or(lock_token),
                started_at: started_at.unwrap_or(now),
            },
        }))
    }

    async fn complete(&self, job: ClaimedJob<Self::Claim>) -> QueueResult<()> {
        let now = now_epoch_seconds()?;
        let processing_ms = elapsed_ms(now, job.claim.started_at);
        let result = self
            .db
            .execute(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "UPDATE jobs
                 SET status = ?,
                     completed_at = ?,
                     locked_at = NULL,
                     lock_token = NULL,
                     last_error = NULL,
                     updated_at = ?,
                     last_finished_at = ?,
                     processing_ms_last = ?,
                     processing_ms_total = processing_ms_total + ?
                 WHERE id = ? AND status = ? AND lock_token = ?"
                    .to_string(),
                vec![
                    STATUS_COMPLETED.into(),
                    now.into(),
                    now.into(),
                    now.into(),
                    processing_ms.into(),
                    processing_ms.into(),
                    job.id.into(),
                    STATUS_PROCESSING.into(),
                    job.claim.lock_token.into(),
                ],
            ))
            .await?;

        ensure_lease(result.rows_affected(), job.id)?;
        Ok(())
    }
}

#[async_trait]
impl RetryableQueue for SeaOrmQueue {
    async fn retry(
        &self,
        job: ClaimedJob<Self::Claim>,
        next_run_at: i64,
        error: String,
    ) -> QueueResult<()> {
        let now = now_epoch_seconds()?;
        let processing_ms = elapsed_ms(now, job.claim.started_at);
        let result = self
            .db
            .execute(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "UPDATE jobs
                 SET status = ?,
                     available_at = ?,
                     locked_at = NULL,
                     lock_token = NULL,
                     last_error = ?,
                     updated_at = ?,
                     last_enqueued_at = ?,
                     last_finished_at = ?,
                     processing_ms_last = ?,
                     processing_ms_total = processing_ms_total + ?
                 WHERE id = ? AND status = ? AND lock_token = ?"
                    .to_string(),
                vec![
                    STATUS_QUEUED.into(),
                    next_run_at.into(),
                    error.into(),
                    now.into(),
                    now.into(),
                    now.into(),
                    processing_ms.into(),
                    processing_ms.into(),
                    job.id.into(),
                    STATUS_PROCESSING.into(),
                    job.claim.lock_token.into(),
                ],
            ))
            .await?;

        ensure_lease(result.rows_affected(), job.id)?;
        Ok(())
    }

    async fn fail(&self, job: ClaimedJob<Self::Claim>, error: String) -> QueueResult<()> {
        let now = now_epoch_seconds()?;
        let processing_ms = elapsed_ms(now, job.claim.started_at);
        let result = self
            .db
            .execute(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "UPDATE jobs
                 SET status = ?,
                     locked_at = NULL,
                     lock_token = NULL,
                     last_error = ?,
                     updated_at = ?,
                     last_finished_at = ?,
                     processing_ms_last = ?,
                     processing_ms_total = processing_ms_total + ?
                 WHERE id = ? AND status = ? AND lock_token = ?"
                    .to_string(),
                vec![
                    STATUS_FAILED.into(),
                    error.into(),
                    now.into(),
                    now.into(),
                    processing_ms.into(),
                    processing_ms.into(),
                    job.id.into(),
                    STATUS_PROCESSING.into(),
                    job.claim.lock_token.into(),
                ],
            ))
            .await?;

        ensure_lease(result.rows_affected(), job.id)?;
        Ok(())
    }
}

#[async_trait]
impl DashboardData for SeaOrmQueue {
    async fn dashboard_stats(&self) -> Result<DashboardStats, BoxError> {
        let row = self
            .db
            .query_one(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT
                    COUNT(*) AS total,
                    COALESCE(SUM(CASE WHEN status = 'queued' THEN 1 ELSE 0 END), 0) AS queued,
                    COALESCE(SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END), 0) AS processing,
                    COALESCE(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END), 0) AS completed,
                    COALESCE(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0) AS failed,
                    COALESCE(SUM(CASE WHEN status = 'cleared' THEN 1 ELSE 0 END), 0) AS cleared
                 FROM jobs"
                    .to_string(),
            ))
            .await?
            .ok_or_else(|| std::io::Error::other("stats query returned no row"))?;

        Ok(DashboardStats {
            total: row.try_get_by_index::<i64>(0)?,
            queued: row.try_get_by_index::<i64>(1)?,
            processing: row.try_get_by_index::<i64>(2)?,
            completed: row.try_get_by_index::<i64>(3)?,
            failed: row.try_get_by_index::<i64>(4)?,
            cleared: row.try_get_by_index::<i64>(5)?,
        })
    }

    async fn dashboard_jobs(&self, limit: i64) -> Result<Vec<DashboardJob>, BoxError> {
        let rows = self
            .db
            .query_all(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                dashboard_jobs_sql().to_string(),
                vec![limit.into()],
            ))
            .await?;

        rows.into_iter().map(dashboard_job_from_seaorm).collect()
    }
}

fn dashboard_job_from_seaorm(row: sea_orm::QueryResult) -> Result<DashboardJob, BoxError> {
    Ok(DashboardJob {
        id: row.try_get_by_index::<i64>(0)?,
        job_type: row.try_get_by_index::<String>(1)?,
        status: row.try_get_by_index::<String>(2)?,
        payload: row.try_get_by_index::<String>(3)?,
        attempts: row.try_get_by_index::<i32>(4)?,
        max_attempts: row.try_get_by_index::<i32>(5)?,
        available_at: row.try_get_by_index::<i64>(6)?,
        locked_at: row.try_get_by_index::<Option<i64>>(7)?,
        last_error: row.try_get_by_index::<Option<String>>(8)?,
        created_at: row.try_get_by_index::<i64>(9)?,
        updated_at: row.try_get_by_index::<i64>(10)?,
        completed_at: row.try_get_by_index::<Option<i64>>(11)?,
        first_enqueued_at: row.try_get_by_index::<Option<i64>>(12)?,
        last_enqueued_at: row.try_get_by_index::<Option<i64>>(13)?,
        first_started_at: row.try_get_by_index::<Option<i64>>(14)?,
        last_started_at: row.try_get_by_index::<Option<i64>>(15)?,
        last_finished_at: row.try_get_by_index::<Option<i64>>(16)?,
        queued_ms_total: row.try_get_by_index::<i64>(17)?,
        queued_ms_last: row.try_get_by_index::<Option<i64>>(18)?,
        processing_ms_total: row.try_get_by_index::<i64>(19)?,
        processing_ms_last: row.try_get_by_index::<Option<i64>>(20)?,
    })
}

fn dashboard_jobs_sql() -> &'static str {
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
}

fn timing_column_definitions() -> [(&'static str, &'static str); 9] {
    [
        ("first_enqueued_at", "INTEGER NULL"),
        ("last_enqueued_at", "INTEGER NULL"),
        ("first_started_at", "INTEGER NULL"),
        ("last_started_at", "INTEGER NULL"),
        ("last_finished_at", "INTEGER NULL"),
        ("queued_ms_total", "INTEGER NOT NULL DEFAULT 0"),
        ("queued_ms_last", "INTEGER NULL"),
        ("processing_ms_total", "INTEGER NOT NULL DEFAULT 0"),
        ("processing_ms_last", "INTEGER NULL"),
    ]
}

fn ensure_lease(rows_affected: u64, job_id: i64) -> QueueResult<()> {
    if rows_affected == 0 {
        return Err(
            std::io::Error::other(format!("lease was lost while processing job {job_id}")).into(),
        );
    }
    Ok(())
}

fn make_worker_id() -> String {
    format!("pid{}", std::process::id())
}

fn now_epoch_seconds() -> Result<i64, std::time::SystemTimeError> {
    let secs = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    Ok(i64::try_from(secs).unwrap_or(i64::MAX))
}

fn duration_to_secs(duration: Duration) -> i64 {
    i64::try_from(duration.as_secs()).unwrap_or(i64::MAX)
}

fn elapsed_ms(now_secs: i64, started_at_secs: i64) -> i64 {
    now_secs
        .saturating_sub(started_at_secs)
        .max(0)
        .saturating_mul(1_000)
}
