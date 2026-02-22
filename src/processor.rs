use crate::{Job, JobError, ProcessorOptions, RunOutcome, entity::jobs};
use sea_orm::{
    ActiveModelTrait, ConnectionTrait, Database, DatabaseConnection, DbBackend, DbErr, Statement,
};
use std::{
    collections::HashSet,
    marker::PhantomData,
    path::Path,
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const STATUS_QUEUED: &str = "queued";
const STATUS_PROCESSING: &str = "processing";
const STATUS_COMPLETED: &str = "completed";
const STATUS_FAILED: &str = "failed";

static CLAIM_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, thiserror::Error)]
pub enum QueueError {
    #[error("database error: {0}")]
    Database(#[from] DbErr),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("row decoding error: {0}")]
    RowDecode(String),
    #[error("clock error: {0}")]
    Clock(#[from] std::time::SystemTimeError),
    #[error("unsupported backend: expected sqlite, got {0:?}")]
    UnsupportedBackend(DbBackend),
    #[error("lease was lost while processing job {0}; increase lock_timeout")]
    LostLease(i64),
    #[error("invalid attempts value in database: {0}")]
    InvalidAttempts(i32),
}

#[derive(Debug)]
struct ClaimedJob<J> {
    id: i64,
    attempts: u32,
    max_attempts: u32,
    started_at: i64,
    lock_token: String,
    job: J,
}

pub struct SqliteJobProcessor<J>
where
    J: Job,
{
    db: DatabaseConnection,
    options: ProcessorOptions,
    worker_id: String,
    enqueue_notify: Arc<tokio::sync::Notify>,
    _marker: PhantomData<J>,
}

#[must_use = "workers are stopped when the handle is dropped"]
pub struct WorkerHandle {
    shutdown: Arc<tokio::sync::Notify>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl WorkerHandle {
    pub fn shutdown(&self) {
        self.shutdown.notify_waiters();
    }

    pub async fn wait(mut self) {
        for task in self.tasks.drain(..) {
            let _ = task.await;
        }
    }

    pub async fn shutdown_and_wait(mut self) {
        self.shutdown.notify_waiters();
        for task in self.tasks.drain(..) {
            let _ = task.await;
        }
    }
}

impl Drop for WorkerHandle {
    fn drop(&mut self) {
        self.shutdown.notify_waiters();
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl<J> Clone for SqliteJobProcessor<J>
where
    J: Job,
{
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            options: self.options.clone(),
            worker_id: self.worker_id.clone(),
            enqueue_notify: Arc::clone(&self.enqueue_notify),
            _marker: PhantomData,
        }
    }
}

impl<J> SqliteJobProcessor<J>
where
    J: Job,
{
    pub async fn connect(
        database_url: &str,
        options: ProcessorOptions,
    ) -> Result<Self, QueueError> {
        let db = Database::connect(database_url).await?;
        Self::new(db, options).await
    }

    pub async fn connect_path(
        path: impl AsRef<Path>,
        options: ProcessorOptions,
    ) -> Result<Self, QueueError> {
        let database_url = format!("sqlite://{}?mode=rwc", path.as_ref().display());
        Self::connect(&database_url, options).await
    }

    pub async fn new(
        db: DatabaseConnection,
        options: ProcessorOptions,
    ) -> Result<Self, QueueError> {
        let backend = db.get_database_backend();
        if backend != DbBackend::Sqlite {
            return Err(QueueError::UnsupportedBackend(backend));
        }

        let processor = Self {
            db,
            options,
            worker_id: make_worker_id(),
            enqueue_notify: Arc::new(tokio::sync::Notify::new()),
            _marker: PhantomData,
        };
        processor.initialize_schema().await?;
        Ok(processor)
    }

    pub fn options(&self) -> &ProcessorOptions {
        &self.options
    }

    pub fn db(&self) -> &DatabaseConnection {
        &self.db
    }

    pub fn spawn_worker(&self) -> WorkerHandle {
        self.spawn_workers(1)
    }

    pub fn spawn_workers(&self, concurrency: usize) -> WorkerHandle {
        let count = concurrency.max(1);
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let mut tasks = Vec::with_capacity(count);

        for _ in 0..count {
            let processor = self.clone();
            let shutdown_signal = Arc::clone(&shutdown);
            let retry_delay = non_zero_poll_interval(self.options.poll_interval);

            tasks.push(tokio::spawn(async move {
                loop {
                    match processor.run_until_notified(shutdown_signal.as_ref()).await {
                        Ok(()) => break,
                        Err(_) => tokio::time::sleep(retry_delay).await,
                    }
                }
            }));
        }

        WorkerHandle { shutdown, tasks }
    }

    pub async fn enqueue(&self, job: &J) -> Result<i64, QueueError> {
        self.enqueue_with_delay(job, Duration::ZERO).await
    }

    pub async fn enqueue_with_delay(&self, job: &J, delay: Duration) -> Result<i64, QueueError> {
        let payload = serde_json::to_string(job)?;
        let now = now_epoch_seconds()?;
        let available_at = now.saturating_add(duration_to_secs(delay));

        let active = jobs::ActiveModel {
            job_type: sea_orm::Set(Self::job_type().to_string()),
            payload: sea_orm::Set(payload),
            status: sea_orm::Set(STATUS_QUEUED.to_string()),
            attempts: sea_orm::Set(0),
            max_attempts: sea_orm::Set(self.options.max_attempts as i32),
            available_at: sea_orm::Set(available_at),
            locked_at: sea_orm::Set(None),
            lock_token: sea_orm::Set(None),
            last_error: sea_orm::Set(None),
            created_at: sea_orm::Set(now),
            updated_at: sea_orm::Set(now),
            completed_at: sea_orm::Set(None),
            first_enqueued_at: sea_orm::Set(Some(now)),
            last_enqueued_at: sea_orm::Set(Some(now)),
            first_started_at: sea_orm::Set(None),
            last_started_at: sea_orm::Set(None),
            last_finished_at: sea_orm::Set(None),
            queued_ms_total: sea_orm::Set(0),
            queued_ms_last: sea_orm::Set(None),
            processing_ms_total: sea_orm::Set(0),
            processing_ms_last: sea_orm::Set(None),
            ..Default::default()
        };

        let model = active.insert(&self.db).await?;
        self.enqueue_notify.notify_one();
        Ok(model.id)
    }

    pub(crate) async fn run_once(&self) -> Result<RunOutcome, QueueError> {
        let now = now_epoch_seconds()?;
        self.reclaim_stale_locks(now).await?;

        let Some(claimed) = self.claim_next_job(now).await? else {
            return Ok(RunOutcome::Idle);
        };

        match claimed.job.process().await {
            Ok(()) => {
                self.mark_completed(claimed.id, &claimed.lock_token, claimed.started_at)
                    .await?;
                Ok(RunOutcome::Completed {
                    job_id: claimed.id,
                    attempts: claimed.attempts,
                })
            }
            Err(job_error) => self.handle_job_error(claimed, job_error).await,
        }
    }

    pub(crate) async fn run_until_notified(
        &self,
        shutdown: &tokio::sync::Notify,
    ) -> Result<(), QueueError> {
        loop {
            match self.run_once().await? {
                RunOutcome::Idle => {
                    let now = now_epoch_seconds()?;
                    let wake_delay = self.next_wakeup_delay(now).await?;
                    if self.wait_for_enqueue_or_shutdown_notify(shutdown, wake_delay).await {
                        return Ok(());
                    }
                }
                _ => {}
            }
        }
    }

    async fn initialize_schema(&self) -> Result<(), QueueError> {
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

    async fn reclaim_stale_locks(&self, now: i64) -> Result<(), QueueError> {
        let stale_before = now.saturating_sub(duration_to_secs(self.options.lock_timeout));
        let statement = Statement::from_sql_and_values(
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
                Self::job_type().into(),
                STATUS_PROCESSING.into(),
                stale_before.into(),
            ],
        );

        self.db.execute(statement).await?;
        Ok(())
    }

    async fn next_wakeup_delay(&self, now: i64) -> Result<Option<Duration>, QueueError> {
        let lock_timeout_secs = duration_to_secs(self.options.lock_timeout);
        let statement = Statement::from_sql_and_values(
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
               AND status IN (?, ?)
            "
                .to_string(),
            vec![
                STATUS_QUEUED.into(),
                STATUS_PROCESSING.into(),
                lock_timeout_secs.into(),
                Self::job_type().into(),
                STATUS_QUEUED.into(),
                STATUS_PROCESSING.into(),
            ],
        );

        let Some(row) = self.db.query_one(statement).await? else {
            return Ok(None);
        };

        let wake_at: Option<i64> = row
            .try_get_by_index(0)
            .map_err(|e| QueueError::RowDecode(format!("{e:?}")))?;
        let Some(wake_at) = wake_at else {
            return Ok(None);
        };

        if wake_at <= now {
            return Ok(Some(non_zero_poll_interval(self.options.poll_interval)));
        }

        let delay_secs = u64::try_from(wake_at.saturating_sub(now)).unwrap_or(u64::MAX);
        Ok(Some(Duration::from_secs(delay_secs)))
    }

    async fn wait_for_enqueue_or_shutdown_notify(
        &self,
        shutdown: &tokio::sync::Notify,
        wake_delay: Option<Duration>,
    ) -> bool {
        match wake_delay {
            Some(delay) => {
                tokio::select! {
                    _ = self.enqueue_notify.notified() => false,
                    _ = tokio::time::sleep(delay) => false,
                    _ = shutdown.notified() => true,
                }
            }
            None => {
                tokio::select! {
                    _ = self.enqueue_notify.notified() => false,
                    _ = shutdown.notified() => true,
                }
            }
        }
    }

    async fn claim_next_job(&self, now: i64) -> Result<Option<ClaimedJob<J>>, QueueError> {
        let lock_token = self.next_lock_token(now);
        let sql = "UPDATE jobs
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
                   RETURNING id, payload, attempts, max_attempts, lock_token, last_started_at";

        let statement = Statement::from_sql_and_values(
            DbBackend::Sqlite,
            sql.to_string(),
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
                Self::job_type().into(),
                STATUS_QUEUED.into(),
                now.into(),
                STATUS_QUEUED.into(),
            ],
        );

        let Some(row) = self.db.query_one(statement).await? else {
            return Ok(None);
        };

        let id: i64 = row
            .try_get_by_index(0)
            .map_err(|e| QueueError::RowDecode(format!("{e:?}")))?;
        let payload: String = row
            .try_get_by_index(1)
            .map_err(|e| QueueError::RowDecode(format!("{e:?}")))?;
        let attempts_raw: i32 = row
            .try_get_by_index(2)
            .map_err(|e| QueueError::RowDecode(format!("{e:?}")))?;
        let max_attempts_raw: i32 = row
            .try_get_by_index(3)
            .map_err(|e| QueueError::RowDecode(format!("{e:?}")))?;
        let stored_lock_token: Option<String> = row
            .try_get_by_index(4)
            .map_err(|e| QueueError::RowDecode(format!("{e:?}")))?;
        let started_at: Option<i64> = row
            .try_get_by_index(5)
            .map_err(|e| QueueError::RowDecode(format!("{e:?}")))?;

        let attempts =
            u32::try_from(attempts_raw).map_err(|_| QueueError::InvalidAttempts(attempts_raw))?;
        let max_attempts = u32::try_from(max_attempts_raw)
            .map_err(|_| QueueError::InvalidAttempts(max_attempts_raw))?;

        let job = serde_json::from_str(&payload)?;

        Ok(Some(ClaimedJob {
            id,
            attempts,
            max_attempts,
            started_at: started_at.unwrap_or(now),
            lock_token: stored_lock_token.unwrap_or(lock_token),
            job,
        }))
    }

    async fn mark_completed(
        &self,
        job_id: i64,
        lock_token: &str,
        started_at: i64,
    ) -> Result<(), QueueError> {
        let now = now_epoch_seconds()?;
        let processing_ms = elapsed_ms(now, started_at);
        let statement = Statement::from_sql_and_values(
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
                job_id.into(),
                STATUS_PROCESSING.into(),
                lock_token.into(),
            ],
        );

        let result = self.db.execute(statement).await?;
        if result.rows_affected() == 0 {
            return Err(QueueError::LostLease(job_id));
        }

        Ok(())
    }

    async fn handle_job_error(
        &self,
        claimed: ClaimedJob<J>,
        job_error: JobError,
    ) -> Result<RunOutcome, QueueError> {
        let error_message = job_error.to_string();
        let retry = job_error.is_retryable() && claimed.attempts < claimed.max_attempts;

        if retry {
            let delay = self.options.backoff.delay_for_attempt(claimed.attempts);
            let next_run_at = now_epoch_seconds()?.saturating_add(duration_to_secs(delay));
            self.mark_retry(
                claimed.id,
                &claimed.lock_token,
                claimed.started_at,
                next_run_at,
                &error_message,
            )
            .await?;

            Ok(RunOutcome::Retried {
                job_id: claimed.id,
                attempts: claimed.attempts,
                next_run_at,
                error: error_message,
            })
        } else {
            self.mark_failed(claimed.id, &claimed.lock_token, claimed.started_at, &error_message)
                .await?;

            Ok(RunOutcome::Failed {
                job_id: claimed.id,
                attempts: claimed.attempts,
                error: error_message,
            })
        }
    }

    async fn mark_retry(
        &self,
        job_id: i64,
        lock_token: &str,
        started_at: i64,
        next_run_at: i64,
        error_message: &str,
    ) -> Result<(), QueueError> {
        let now = now_epoch_seconds()?;
        let processing_ms = elapsed_ms(now, started_at);
        let statement = Statement::from_sql_and_values(
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
                error_message.into(),
                now.into(),
                now.into(),
                now.into(),
                processing_ms.into(),
                processing_ms.into(),
                job_id.into(),
                STATUS_PROCESSING.into(),
                lock_token.into(),
            ],
        );

        let result = self.db.execute(statement).await?;
        if result.rows_affected() == 0 {
            return Err(QueueError::LostLease(job_id));
        }

        Ok(())
    }

    async fn mark_failed(
        &self,
        job_id: i64,
        lock_token: &str,
        started_at: i64,
        error_message: &str,
    ) -> Result<(), QueueError> {
        let now = now_epoch_seconds()?;
        let processing_ms = elapsed_ms(now, started_at);
        let statement = Statement::from_sql_and_values(
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
                error_message.into(),
                now.into(),
                now.into(),
                processing_ms.into(),
                processing_ms.into(),
                job_id.into(),
                STATUS_PROCESSING.into(),
                lock_token.into(),
            ],
        );

        let result = self.db.execute(statement).await?;
        if result.rows_affected() == 0 {
            return Err(QueueError::LostLease(job_id));
        }

        Ok(())
    }

    fn next_lock_token(&self, now: i64) -> String {
        let counter = CLAIM_COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("{}-{}-{}", self.worker_id, now, counter)
    }

    fn job_type() -> &'static str {
        std::any::type_name::<J>()
    }

    async fn ensure_timing_columns(&self) -> Result<(), QueueError> {
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

    async fn job_columns(&self) -> Result<HashSet<String>, QueueError> {
        let rows = self
            .db
            .query_all(Statement::from_string(
                DbBackend::Sqlite,
                "PRAGMA table_info(jobs)".to_string(),
            ))
            .await?;

        let mut columns = HashSet::with_capacity(rows.len());
        for row in rows {
            let column: String = row
                .try_get_by_index(1)
                .map_err(|e| QueueError::RowDecode(format!("{e:?}")))?;
            columns.insert(column);
        }

        Ok(columns)
    }
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

fn non_zero_poll_interval(interval: Duration) -> Duration {
    if interval.is_zero() {
        Duration::from_millis(1)
    } else {
        interval
    }
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
