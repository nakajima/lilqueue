use async_trait::async_trait;

pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type QueueResult<T> = Result<T, BoxError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewJob {
    pub job_type: String,
    pub payload: String,
    pub available_at: i64,
    pub max_attempts: u32,
    pub enqueued_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedJob<C> {
    pub id: i64,
    pub payload: String,
    pub attempts: u32,
    pub max_attempts: u32,
    pub claim: C,
}

/// Enqueue-only queue capability.
#[async_trait]
pub trait JobQueue: Clone + Send + Sync + 'static {
    async fn enqueue(&self, job: NewJob) -> QueueResult<i64>;

    async fn next_wakeup_at(&self, _job_type: &str) -> QueueResult<Option<i64>> {
        Ok(None)
    }
}

/// Claim-and-complete queue capability.
///
/// `claim` must atomically make the returned job unavailable to other workers.
/// Any lock token, lease timeout, or backend-specific state belongs in `Claim`.
#[async_trait]
pub trait LockableQueue: JobQueue {
    type Claim: Send + Sync + 'static;

    async fn claim(&self, job_type: &str) -> QueueResult<Option<ClaimedJob<Self::Claim>>>;

    async fn complete(&self, job: ClaimedJob<Self::Claim>) -> QueueResult<()>;
}

/// Retry-and-fail queue capability used by `JobProcessor` workers.
#[async_trait]
pub trait RetryableQueue: LockableQueue {
    async fn retry(
        &self,
        job: ClaimedJob<Self::Claim>,
        next_run_at: i64,
        error: String,
    ) -> QueueResult<()>;

    async fn fail(&self, job: ClaimedJob<Self::Claim>, error: String) -> QueueResult<()>;
}
