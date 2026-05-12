use async_trait::async_trait;
use lilqueue::{
    BoxError, ClaimedJob, JobQueue, LockableQueue, NewJob, QueueResult, RetryableQueue,
    dashboard::{DashboardData, DashboardJob, DashboardStats},
};
use rusqlite::params;
use seekwel::{connection::Connection, error::Error as SeekwelError};
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
pub struct SeekwelQueueOptions {
    pub lock_timeout: Duration,
}

impl Default for SeekwelQueueOptions {
    fn default() -> Self {
        Self {
            lock_timeout: Duration::from_secs(300),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SeekwelQueue {
    connection: Connection,
    options: SeekwelQueueOptions,
    worker_id: String,
    claim_counter: Arc<AtomicU64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeekwelClaim {
    pub lock_token: String,
    pub started_at: i64,
}

impl SeekwelQueue {
    pub fn global(options: SeekwelQueueOptions) -> Result<Self, SeekwelError> {
        Self::new(Connection::get()?, options)
    }

    pub fn new(connection: Connection, options: SeekwelQueueOptions) -> Result<Self, SeekwelError> {
        let queue = Self {
            connection,
            options,
            worker_id: make_worker_id(),
            claim_counter: Arc::new(AtomicU64::new(1)),
        };
        queue.initialize_schema()?;
        Ok(queue)
    }

    pub fn connection(&self) -> Connection {
        self.connection
    }

    pub fn options(&self) -> &SeekwelQueueOptions {
        &self.options
    }

    fn initialize_schema(&self) -> Result<(), SeekwelError> {
        self.connection.execute(
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
            )",
            (),
        )?;

        self.ensure_timing_columns()?;

        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_jobs_ready
                ON jobs (job_type, status, available_at, id)",
            (),
        )?;

        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_jobs_processing
                ON jobs (job_type, status, locked_at)",
            (),
        )?;

        Ok(())
    }

    fn ensure_timing_columns(&self) -> Result<(), SeekwelError> {
        let existing = self.job_columns()?;
        for (column, definition) in timing_column_definitions() {
            if !existing.contains(column) {
                self.connection.execute(
                    &format!("ALTER TABLE jobs ADD COLUMN {column} {definition}"),
                    (),
                )?;
            }
        }

        self.connection.execute(
            "UPDATE jobs
             SET first_enqueued_at = COALESCE(first_enqueued_at, created_at)",
            (),
        )?;

        Ok(())
    }

    fn job_columns(&self) -> Result<HashSet<String>, SeekwelError> {
        let rows = self
            .connection
            .query_all("PRAGMA table_info(jobs)", (), |row| row.get::<_, String>(1))?;
        Ok(rows.into_iter().collect())
    }

    fn reclaim_stale_locks(&self, job_type: &str, now: i64) -> Result<(), SeekwelError> {
        let stale_before = now.saturating_sub(duration_to_secs(self.options.lock_timeout));
        self.connection.execute(
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
               AND locked_at <= ?",
            params![
                STATUS_QUEUED,
                now,
                now,
                now,
                now,
                now,
                now,
                now,
                now,
                now,
                now,
                now,
                job_type,
                STATUS_PROCESSING,
                stale_before,
            ],
        )?;
        Ok(())
    }

    fn next_lock_token(&self, now: i64) -> String {
        let counter = self.claim_counter.fetch_add(1, Ordering::Relaxed);
        format!("{}-{}-{}", self.worker_id, now, counter)
    }
}

#[async_trait]
impl JobQueue for SeekwelQueue {
    async fn enqueue(&self, job: NewJob) -> QueueResult<i64> {
        let id = self.connection.query_row(
            "INSERT INTO jobs
             (job_type, payload, status, attempts, max_attempts, available_at, locked_at,
              lock_token, last_error, created_at, updated_at, completed_at,
              first_enqueued_at, last_enqueued_at, first_started_at, last_started_at,
              last_finished_at, queued_ms_total, queued_ms_last, processing_ms_total,
              processing_ms_last)
             VALUES (?, ?, ?, 0, ?, ?, NULL, NULL, NULL, ?, ?, NULL, ?, ?, NULL, NULL,
                     NULL, 0, NULL, 0, NULL)
             RETURNING id",
            params![
                job.job_type,
                job.payload,
                STATUS_QUEUED,
                i64::from(job.max_attempts),
                job.available_at,
                job.enqueued_at,
                job.enqueued_at,
                job.enqueued_at,
                job.enqueued_at,
            ],
            |row| row.get::<_, i64>(0),
        )?;
        Ok(id)
    }

    async fn next_wakeup_at(&self, job_type: &str) -> QueueResult<Option<i64>> {
        let lock_timeout_secs = duration_to_secs(self.options.lock_timeout);
        Ok(self
            .connection
            .query_optional(
                "SELECT MIN(
                    CASE
                        WHEN status = ? THEN available_at
                        WHEN status = ? AND locked_at IS NOT NULL THEN locked_at + ?
                        ELSE NULL
                    END
                 )
                 FROM jobs
                 WHERE job_type = ?
                   AND status IN (?, ?)",
                params![
                    STATUS_QUEUED,
                    STATUS_PROCESSING,
                    lock_timeout_secs,
                    job_type,
                    STATUS_QUEUED,
                    STATUS_PROCESSING,
                ],
                |row| row.get::<_, Option<i64>>(0),
            )?
            .flatten())
    }
}

#[async_trait]
impl LockableQueue for SeekwelQueue {
    type Claim = SeekwelClaim;

    async fn claim(&self, job_type: &str) -> QueueResult<Option<ClaimedJob<Self::Claim>>> {
        let now = now_epoch_seconds()?;
        let lock_token = self.next_lock_token(now);

        let claimed = Connection::transaction(|| {
            self.reclaim_stale_locks(job_type, now)?;
            self.connection.query_optional(
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
                 RETURNING id, payload, attempts, max_attempts, lock_token, last_started_at",
                params![
                    STATUS_PROCESSING,
                    now,
                    lock_token,
                    now,
                    now,
                    now,
                    now,
                    now,
                    now,
                    now,
                    now,
                    now,
                    now,
                    now,
                    job_type,
                    STATUS_QUEUED,
                    now,
                    STATUS_QUEUED,
                ],
                |row| {
                    let id = row.get::<_, i64>(0)?;
                    let payload = row.get::<_, String>(1)?;
                    let attempts = row.get::<_, i64>(2)?;
                    let max_attempts = row.get::<_, i64>(3)?;
                    let stored_lock_token = row.get::<_, Option<String>>(4)?;
                    let started_at = row.get::<_, Option<i64>>(5)?;
                    Ok((
                        id,
                        payload,
                        attempts,
                        max_attempts,
                        stored_lock_token,
                        started_at,
                    ))
                },
            )
        })?;

        let Some((id, payload, attempts, max_attempts, stored_lock_token, started_at)) = claimed
        else {
            return Ok(None);
        };

        Ok(Some(ClaimedJob {
            id,
            payload,
            attempts: u32::try_from(attempts)?,
            max_attempts: u32::try_from(max_attempts)?,
            claim: SeekwelClaim {
                lock_token: stored_lock_token.unwrap_or(lock_token),
                started_at: started_at.unwrap_or(now),
            },
        }))
    }

    async fn complete(&self, job: ClaimedJob<Self::Claim>) -> QueueResult<()> {
        let now = now_epoch_seconds()?;
        let processing_ms = elapsed_ms(now, job.claim.started_at);
        let rows_affected = self.connection.execute(
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
             WHERE id = ? AND status = ? AND lock_token = ?",
            params![
                STATUS_COMPLETED,
                now,
                now,
                now,
                processing_ms,
                processing_ms,
                job.id,
                STATUS_PROCESSING,
                job.claim.lock_token,
            ],
        )?;

        ensure_lease(rows_affected, job.id)?;
        Ok(())
    }
}

#[async_trait]
impl RetryableQueue for SeekwelQueue {
    async fn retry(
        &self,
        job: ClaimedJob<Self::Claim>,
        next_run_at: i64,
        error: String,
    ) -> QueueResult<()> {
        let now = now_epoch_seconds()?;
        let processing_ms = elapsed_ms(now, job.claim.started_at);
        let rows_affected = self.connection.execute(
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
             WHERE id = ? AND status = ? AND lock_token = ?",
            params![
                STATUS_QUEUED,
                next_run_at,
                error,
                now,
                now,
                now,
                processing_ms,
                processing_ms,
                job.id,
                STATUS_PROCESSING,
                job.claim.lock_token,
            ],
        )?;

        ensure_lease(rows_affected, job.id)?;
        Ok(())
    }

    async fn fail(&self, job: ClaimedJob<Self::Claim>, error: String) -> QueueResult<()> {
        let now = now_epoch_seconds()?;
        let processing_ms = elapsed_ms(now, job.claim.started_at);
        let rows_affected = self.connection.execute(
            "UPDATE jobs
             SET status = ?,
                 locked_at = NULL,
                 lock_token = NULL,
                 last_error = ?,
                 updated_at = ?,
                 last_finished_at = ?,
                 processing_ms_last = ?,
                 processing_ms_total = processing_ms_total + ?
             WHERE id = ? AND status = ? AND lock_token = ?",
            params![
                STATUS_FAILED,
                error,
                now,
                now,
                processing_ms,
                processing_ms,
                job.id,
                STATUS_PROCESSING,
                job.claim.lock_token,
            ],
        )?;

        ensure_lease(rows_affected, job.id)?;
        Ok(())
    }
}

#[async_trait]
impl DashboardData for SeekwelQueue {
    async fn dashboard_stats(&self) -> Result<DashboardStats, BoxError> {
        Ok(self.connection.query_row(
            "SELECT
                COUNT(*) AS total,
                COALESCE(SUM(CASE WHEN status = 'queued' THEN 1 ELSE 0 END), 0) AS queued,
                COALESCE(SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END), 0) AS processing,
                COALESCE(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END), 0) AS completed,
                COALESCE(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0) AS failed,
                COALESCE(SUM(CASE WHEN status = 'cleared' THEN 1 ELSE 0 END), 0) AS cleared
             FROM jobs",
            (),
            |row| {
                Ok(DashboardStats {
                    total: row.get(0)?,
                    queued: row.get(1)?,
                    processing: row.get(2)?,
                    completed: row.get(3)?,
                    failed: row.get(4)?,
                    cleared: row.get(5)?,
                })
            },
        )?)
    }

    async fn dashboard_jobs(&self, limit: i64) -> Result<Vec<DashboardJob>, BoxError> {
        Ok(self.connection.query_all(
            dashboard_jobs_sql(),
            params![limit],
            dashboard_job_from_row,
        )?)
    }
}

fn dashboard_job_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<DashboardJob> {
    Ok(DashboardJob {
        id: row.get(0)?,
        job_type: row.get(1)?,
        status: row.get(2)?,
        payload: row.get(3)?,
        attempts: row.get(4)?,
        max_attempts: row.get(5)?,
        available_at: row.get(6)?,
        locked_at: row.get(7)?,
        last_error: row.get(8)?,
        created_at: row.get(9)?,
        updated_at: row.get(10)?,
        completed_at: row.get(11)?,
        first_enqueued_at: row.get(12)?,
        last_enqueued_at: row.get(13)?,
        first_started_at: row.get(14)?,
        last_started_at: row.get(15)?,
        last_finished_at: row.get(16)?,
        queued_ms_total: row.get(17)?,
        queued_ms_last: row.get(18)?,
        processing_ms_total: row.get(19)?,
        processing_ms_last: row.get(20)?,
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

fn ensure_lease(rows_affected: usize, job_id: i64) -> QueueResult<()> {
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
