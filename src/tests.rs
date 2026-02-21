use super::*;
use async_trait::async_trait;
use sea_orm::{ConnectionTrait, Database, DbBackend, Statement};
use std::{
    fs,
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tempfile::tempdir;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct WriteFileJob {
    output_path: String,
    line: String,
}

#[async_trait]
impl Job for WriteFileJob {
    async fn process(&self) -> Result<(), JobError> {
        use std::io::Write;

        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.output_path)
            .map_err(|e| JobError::permanent(e.to_string()))?;
        writeln!(file, "{}", self.line).map_err(|e| JobError::permanent(e.to_string()))?;
        Ok(())
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct FlakyJob {
    state_path: String,
    succeed_on_attempt: u32,
}

#[async_trait]
impl Job for FlakyJob {
    async fn process(&self) -> Result<(), JobError> {
        let attempt = fs::read_to_string(&self.state_path)
            .ok()
            .and_then(|v| v.trim().parse::<u32>().ok())
            .unwrap_or(0)
            .saturating_add(1);

        fs::write(&self.state_path, attempt.to_string())
            .map_err(|e| JobError::permanent(e.to_string()))?;

        if attempt < self.succeed_on_attempt {
            return Err(JobError::retryable("transient failure"));
        }

        Ok(())
    }
}

#[tokio::test]
async fn processes_successful_job() {
    let dir = tempdir().unwrap();
    let db_url = sqlite_url(dir.path().join("queue.db"));

    let processor = SqliteJobProcessor::<WriteFileJob>::connect(&db_url, test_options())
        .await
        .unwrap();

    let output_path = dir.path().join("output.log");
    let job = WriteFileJob {
        output_path: output_path.to_string_lossy().to_string(),
        line: "hello".to_string(),
    };

    let id = processor.enqueue(&job).await.unwrap();
    let outcome = processor.run_once().await.unwrap();

    assert_eq!(
        outcome,
        RunOutcome::Completed {
            job_id: id,
            attempts: 1,
        }
    );

    let file_contents = fs::read_to_string(output_path).unwrap();
    assert!(file_contents.contains("hello"));

    let idle = processor.run_once().await.unwrap();
    assert_eq!(idle, RunOutcome::Idle);
}

#[tokio::test]
async fn retries_and_then_completes() {
    let dir = tempdir().unwrap();
    let db_url = sqlite_url(dir.path().join("queue.db"));

    let mut options = test_options();
    options.max_attempts = 5;
    options.backoff = BackoffStrategy::Fixed(Duration::ZERO);

    let processor = SqliteJobProcessor::<FlakyJob>::connect(&db_url, options)
        .await
        .unwrap();

    let state_path = dir.path().join("attempt.txt");
    let job = FlakyJob {
        state_path: state_path.to_string_lossy().to_string(),
        succeed_on_attempt: 2,
    };

    processor.enqueue(&job).await.unwrap();

    let first = processor.run_once().await.unwrap();
    assert!(matches!(first, RunOutcome::Retried { attempts: 1, .. }));

    let second = processor.run_once().await.unwrap();
    assert!(matches!(second, RunOutcome::Completed { attempts: 2, .. }));

    let attempts_file = fs::read_to_string(state_path).unwrap();
    assert_eq!(attempts_file.trim(), "2");
}

#[tokio::test]
async fn reclaims_stale_processing_job_after_crash() {
    let dir = tempdir().unwrap();
    let db_url = sqlite_url(dir.path().join("queue.db"));

    let mut options = test_options();
    options.lock_timeout = Duration::from_secs(1);
    let processor = SqliteJobProcessor::<WriteFileJob>::connect(&db_url, options)
        .await
        .unwrap();

    let output_path = dir.path().join("stale.log");
    let job = WriteFileJob {
        output_path: output_path.to_string_lossy().to_string(),
        line: "recovered".to_string(),
    };
    let job_id = processor.enqueue(&job).await.unwrap();

    let stale_time = current_epoch_seconds().saturating_sub(60);
    processor
        .db()
        .execute(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE jobs SET status = ?, locked_at = ?, lock_token = ? WHERE id = ?".to_string(),
            vec![
                "processing".into(),
                stale_time.into(),
                "dead-worker".into(),
                job_id.into(),
            ],
        ))
        .await
        .unwrap();

    let outcome = processor.run_once().await.unwrap();
    assert!(matches!(outcome, RunOutcome::Completed { job_id: id, .. } if id == job_id));

    let file_contents = fs::read_to_string(output_path).unwrap();
    assert!(file_contents.contains("recovered"));
}

#[tokio::test]
async fn fails_permanent_job_without_retry() {
    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    struct PermanentFailJob;

    #[async_trait]
    impl Job for PermanentFailJob {
        async fn process(&self) -> Result<(), JobError> {
            Err(JobError::permanent("bad payload"))
        }
    }

    let dir = tempdir().unwrap();
    let db_url = sqlite_url(dir.path().join("queue.db"));
    let processor = SqliteJobProcessor::<PermanentFailJob>::connect(&db_url, test_options())
        .await
        .unwrap();

    let job_id = processor.enqueue(&PermanentFailJob).await.unwrap();
    let outcome = processor.run_once().await.unwrap();

    assert_eq!(
        outcome,
        RunOutcome::Failed {
            job_id,
            attempts: 1,
            error: "bad payload".to_string(),
        }
    );
}

#[tokio::test]
async fn connect_path_works() {
    let dir = tempdir().unwrap();
    let processor = SqliteJobProcessor::<WriteFileJob>::connect_path(
        dir.path().join("queue.db"),
        test_options(),
    )
    .await
    .unwrap();

    let db = Database::connect(&sqlite_url(dir.path().join("queue.db")))
        .await
        .unwrap();
    let count = db
        .query_one(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT COUNT(*) as count FROM jobs".to_string(),
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get_by_index::<i64>(0)
        .unwrap();

    assert_eq!(count, 0);
    drop(processor);
}

#[tokio::test]
async fn run_until_notified_wakes_when_job_is_enqueued() {
    let dir = tempdir().unwrap();
    let db_url = sqlite_url(dir.path().join("queue.db"));

    let mut options = test_options();
    options.poll_interval = Duration::from_secs(60);

    let processor = SqliteJobProcessor::<WriteFileJob>::connect(&db_url, options)
        .await
        .unwrap();

    let output_path = dir.path().join("notify.log");
    let output_path_str = output_path.to_string_lossy().to_string();

    let shutdown = Arc::new(tokio::sync::Notify::new());
    let worker_processor = processor.clone();
    let worker_shutdown = Arc::clone(&shutdown);
    let worker = tokio::spawn(async move {
        worker_processor
            .run_until_notified(worker_shutdown.as_ref())
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    processor
        .enqueue(&WriteFileJob {
            output_path: output_path_str,
            line: "wake".to_string(),
        })
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if fs::read_to_string(&output_path)
                .map(|contents| contents.contains("wake"))
                .unwrap_or(false)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    shutdown.notify_waiters();
    tokio::time::timeout(Duration::from_secs(2), worker)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn spawn_worker_processes_job_and_can_shutdown() {
    let dir = tempdir().unwrap();
    let db_url = sqlite_url(dir.path().join("queue.db"));

    let mut options = test_options();
    options.poll_interval = Duration::from_secs(60);

    let processor = SqliteJobProcessor::<WriteFileJob>::connect(&db_url, options)
        .await
        .unwrap();
    let worker = processor.spawn_worker();

    let output_path = dir.path().join("spawn.log");
    processor
        .enqueue(&WriteFileJob {
            output_path: output_path.to_string_lossy().to_string(),
            line: "spawned".to_string(),
        })
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if fs::read_to_string(&output_path)
                .map(|contents| contents.contains("spawned"))
                .unwrap_or(false)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    tokio::time::timeout(Duration::from_secs(2), worker.shutdown_and_wait())
        .await
        .unwrap();
}

fn test_options() -> ProcessorOptions {
    ProcessorOptions {
        max_attempts: 3,
        lock_timeout: Duration::from_secs(30),
        poll_interval: Duration::from_millis(1),
        backoff: BackoffStrategy::Fixed(Duration::ZERO),
    }
}

fn sqlite_url(path: impl AsRef<Path>) -> String {
    format!("sqlite://{}?mode=rwc", path.as_ref().display())
}

fn current_epoch_seconds() -> i64 {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    i64::try_from(secs).unwrap_or(i64::MAX)
}
