use crate::{
    ClaimedJob, Job, JobQueue, NewJob, ProcessorOptions, RetryableQueue, RunOutcome,
    queue::BoxError,
};
use std::{
    marker::PhantomData,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Debug, thiserror::Error)]
pub enum QueueError {
    #[error("queue error: {0}")]
    Queue(#[from] BoxError),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("clock error: {0}")]
    Clock(#[from] std::time::SystemTimeError),
}

pub struct JobProcessor<J, Q>
where
    J: Job,
    Q: JobQueue,
{
    queue: Q,
    options: ProcessorOptions,
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

impl<J, Q> Clone for JobProcessor<J, Q>
where
    J: Job,
    Q: JobQueue,
{
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
            options: self.options.clone(),
            enqueue_notify: Arc::clone(&self.enqueue_notify),
            _marker: PhantomData,
        }
    }
}

impl<J, Q> JobProcessor<J, Q>
where
    J: Job,
    Q: JobQueue,
{
    pub fn new(queue: Q, options: ProcessorOptions) -> Self {
        Self {
            queue,
            options,
            enqueue_notify: Arc::new(tokio::sync::Notify::new()),
            _marker: PhantomData,
        }
    }

    pub fn options(&self) -> &ProcessorOptions {
        &self.options
    }

    pub fn queue(&self) -> &Q {
        &self.queue
    }

    pub fn wake_workers(&self) {
        self.enqueue_notify.notify_waiters();
    }

    pub async fn enqueue(&self, job: &J) -> Result<i64, QueueError> {
        self.enqueue_with_delay(job, Duration::ZERO).await
    }

    pub async fn enqueue_with_delay(&self, job: &J, delay: Duration) -> Result<i64, QueueError> {
        let now = now_epoch_seconds()?;
        let payload = serde_json::to_string(job)?;
        let id = self
            .queue
            .enqueue(NewJob {
                job_type: J::job_type().to_string(),
                payload,
                available_at: now.saturating_add(duration_to_secs(delay)),
                max_attempts: self.options.max_attempts,
                enqueued_at: now,
            })
            .await?;
        self.enqueue_notify.notify_one();
        Ok(id)
    }
}

impl<J, Q> JobProcessor<J, Q>
where
    J: Job,
    Q: RetryableQueue,
{
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

    pub(crate) async fn run_once(&self) -> Result<RunOutcome, QueueError> {
        let Some(claimed) = self.queue.claim(J::job_type()).await? else {
            return Ok(RunOutcome::Idle);
        };

        let job_id = claimed.id;
        let attempts = claimed.attempts;
        let max_attempts = claimed.max_attempts;
        let job = serde_json::from_str::<J>(&claimed.payload)?;

        match job.process().await {
            Ok(()) => {
                self.queue.complete(claimed).await?;
                Ok(RunOutcome::Completed { job_id, attempts })
            }
            Err(job_error) => {
                self.handle_job_error(claimed, job_id, attempts, max_attempts, job_error)
                    .await
            }
        }
    }

    pub(crate) async fn run_until_notified(
        &self,
        shutdown: &tokio::sync::Notify,
    ) -> Result<(), QueueError> {
        loop {
            match self.run_once().await? {
                RunOutcome::Idle => {
                    let wake_delay = self.next_wakeup_delay().await?;
                    if self
                        .wait_for_enqueue_or_shutdown_notify(shutdown, wake_delay)
                        .await
                    {
                        return Ok(());
                    }
                }
                _ => {}
            }
        }
    }

    async fn handle_job_error(
        &self,
        claimed: ClaimedJob<Q::Claim>,
        job_id: i64,
        attempts: u32,
        max_attempts: u32,
        job_error: crate::JobError,
    ) -> Result<RunOutcome, QueueError> {
        let error_message = job_error.to_string();
        let retry = job_error.is_retryable() && attempts < max_attempts;

        if retry {
            let delay = self.options.backoff.delay_for_attempt(attempts);
            let next_run_at = now_epoch_seconds()?.saturating_add(duration_to_secs(delay));
            self.queue
                .retry(claimed, next_run_at, error_message.clone())
                .await?;

            Ok(RunOutcome::Retried {
                job_id,
                attempts,
                next_run_at,
                error: error_message,
            })
        } else {
            self.queue.fail(claimed, error_message.clone()).await?;

            Ok(RunOutcome::Failed {
                job_id,
                attempts,
                error: error_message,
            })
        }
    }

    async fn next_wakeup_delay(&self) -> Result<Option<Duration>, QueueError> {
        let Some(wake_at) = self.queue.next_wakeup_at(J::job_type()).await? else {
            return Ok(None);
        };
        let now = now_epoch_seconds()?;

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
}

fn now_epoch_seconds() -> Result<i64, std::time::SystemTimeError> {
    let secs = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    Ok(i64::try_from(secs).unwrap_or(i64::MAX))
}

fn duration_to_secs(duration: Duration) -> i64 {
    i64::try_from(duration.as_secs()).unwrap_or(i64::MAX)
}

fn non_zero_poll_interval(interval: Duration) -> Duration {
    if interval.is_zero() {
        Duration::from_millis(1)
    } else {
        interval
    }
}
