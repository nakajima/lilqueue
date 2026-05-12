use super::*;
use async_trait::async_trait;
use std::{
    fs,
    sync::{Arc, Mutex},
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
    let queue = MemoryQueue::default();
    let processor = JobProcessor::<WriteFileJob, _>::new(queue.clone(), test_options());

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
    assert_eq!(queue.job(id).status, MemoryStatus::Completed);

    let idle = processor.run_once().await.unwrap();
    assert_eq!(idle, RunOutcome::Idle);
}

#[tokio::test]
async fn retries_and_then_completes() {
    let dir = tempdir().unwrap();
    let queue = MemoryQueue::default();

    let mut options = test_options();
    options.max_attempts = 5;
    options.backoff = BackoffStrategy::Fixed(Duration::ZERO);

    let processor = JobProcessor::<FlakyJob, _>::new(queue.clone(), options);

    let state_path = dir.path().join("attempt.txt");
    let job = FlakyJob {
        state_path: state_path.to_string_lossy().to_string(),
        succeed_on_attempt: 2,
    };

    let job_id = processor.enqueue(&job).await.unwrap();

    let first = processor.run_once().await.unwrap();
    assert!(matches!(first, RunOutcome::Retried { attempts: 1, .. }));
    assert_eq!(queue.job(job_id).status, MemoryStatus::Queued);
    assert_eq!(
        queue.job(job_id).last_error.as_deref(),
        Some("transient failure")
    );

    let second = processor.run_once().await.unwrap();
    assert!(matches!(second, RunOutcome::Completed { attempts: 2, .. }));
    assert_eq!(queue.job(job_id).status, MemoryStatus::Completed);

    let attempts_file = fs::read_to_string(state_path).unwrap();
    assert_eq!(attempts_file.trim(), "2");
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

    let queue = MemoryQueue::default();
    let processor = JobProcessor::<PermanentFailJob, _>::new(queue.clone(), test_options());

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
    assert_eq!(queue.job(job_id).status, MemoryStatus::Failed);
}

#[tokio::test]
async fn delayed_job_is_not_claimed_until_available() {
    let dir = tempdir().unwrap();
    let queue = MemoryQueue::default();
    let processor = JobProcessor::<WriteFileJob, _>::new(queue.clone(), test_options());

    let output_path = dir.path().join("delayed.log");
    processor
        .enqueue_with_delay(
            &WriteFileJob {
                output_path: output_path.to_string_lossy().to_string(),
                line: "delayed".to_string(),
            },
            Duration::from_secs(60),
        )
        .await
        .unwrap();

    let outcome = processor.run_once().await.unwrap();
    assert_eq!(outcome, RunOutcome::Idle);
    assert!(!output_path.exists());
}

#[tokio::test]
async fn run_until_notified_wakes_when_job_is_enqueued() {
    let dir = tempdir().unwrap();
    let queue = MemoryQueue::default();

    let mut options = test_options();
    options.poll_interval = Duration::from_secs(60);

    let processor = JobProcessor::<WriteFileJob, _>::new(queue, options);

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
    let queue = MemoryQueue::default();

    let mut options = test_options();
    options.poll_interval = Duration::from_secs(60);

    let processor = JobProcessor::<WriteFileJob, _>::new(queue, options);
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
        poll_interval: Duration::from_millis(1),
        backoff: BackoffStrategy::Fixed(Duration::ZERO),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MemoryStatus {
    Queued,
    Processing,
    Completed,
    Failed,
}

#[derive(Debug, Clone)]
struct MemoryJob {
    id: i64,
    job_type: String,
    payload: String,
    status: MemoryStatus,
    attempts: u32,
    max_attempts: u32,
    available_at: i64,
    last_error: Option<String>,
}

#[derive(Debug, Clone)]
struct MemoryClaim {
    id: i64,
}

#[derive(Debug, Default)]
struct MemoryState {
    next_id: i64,
    jobs: Vec<MemoryJob>,
}

#[derive(Debug, Clone, Default)]
struct MemoryQueue {
    inner: Arc<Mutex<MemoryState>>,
}

impl MemoryQueue {
    fn job(&self, id: i64) -> MemoryJob {
        self.inner
            .lock()
            .unwrap()
            .jobs
            .iter()
            .find(|job| job.id == id)
            .unwrap()
            .clone()
    }
}

#[async_trait]
impl JobQueue for MemoryQueue {
    async fn enqueue(&self, job: NewJob) -> QueueResult<i64> {
        let mut state = self.inner.lock().unwrap();
        state.next_id = state.next_id.saturating_add(1);
        let id = state.next_id;
        state.jobs.push(MemoryJob {
            id,
            job_type: job.job_type,
            payload: job.payload,
            status: MemoryStatus::Queued,
            attempts: 0,
            max_attempts: job.max_attempts,
            available_at: job.available_at,
            last_error: None,
        });
        Ok(id)
    }

    async fn next_wakeup_at(&self, job_type: &str) -> QueueResult<Option<i64>> {
        let state = self.inner.lock().unwrap();
        Ok(state
            .jobs
            .iter()
            .filter(|job| job.job_type == job_type && job.status == MemoryStatus::Queued)
            .map(|job| job.available_at)
            .min())
    }
}

#[async_trait]
impl LockableQueue for MemoryQueue {
    type Claim = MemoryClaim;

    async fn claim(&self, job_type: &str) -> QueueResult<Option<ClaimedJob<Self::Claim>>> {
        let now = current_epoch_seconds();
        let mut state = self.inner.lock().unwrap();
        let Some(job) = state
            .jobs
            .iter_mut()
            .filter(|job| {
                job.job_type == job_type
                    && job.status == MemoryStatus::Queued
                    && job.available_at <= now
            })
            .min_by_key(|job| (job.available_at, job.id))
        else {
            return Ok(None);
        };

        job.status = MemoryStatus::Processing;
        job.attempts = job.attempts.saturating_add(1);

        Ok(Some(ClaimedJob {
            id: job.id,
            payload: job.payload.clone(),
            attempts: job.attempts,
            max_attempts: job.max_attempts,
            claim: MemoryClaim { id: job.id },
        }))
    }

    async fn complete(&self, job: ClaimedJob<Self::Claim>) -> QueueResult<()> {
        let mut state = self.inner.lock().unwrap();
        let Some(stored) = state
            .jobs
            .iter_mut()
            .find(|stored| stored.id == job.claim.id)
        else {
            return Err(std::io::Error::other("claimed job not found").into());
        };
        stored.status = MemoryStatus::Completed;
        stored.last_error = None;
        Ok(())
    }
}

#[async_trait]
impl RetryableQueue for MemoryQueue {
    async fn retry(
        &self,
        job: ClaimedJob<Self::Claim>,
        next_run_at: i64,
        error: String,
    ) -> QueueResult<()> {
        let mut state = self.inner.lock().unwrap();
        let Some(stored) = state
            .jobs
            .iter_mut()
            .find(|stored| stored.id == job.claim.id)
        else {
            return Err(std::io::Error::other("claimed job not found").into());
        };
        stored.status = MemoryStatus::Queued;
        stored.available_at = next_run_at;
        stored.last_error = Some(error);
        Ok(())
    }

    async fn fail(&self, job: ClaimedJob<Self::Claim>, error: String) -> QueueResult<()> {
        let mut state = self.inner.lock().unwrap();
        let Some(stored) = state
            .jobs
            .iter_mut()
            .find(|stored| stored.id == job.claim.id)
        else {
            return Err(std::io::Error::other("claimed job not found").into());
        };
        stored.status = MemoryStatus::Failed;
        stored.last_error = Some(error);
        Ok(())
    }
}

fn current_epoch_seconds() -> i64 {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    i64::try_from(secs).unwrap_or(i64::MAX)
}
