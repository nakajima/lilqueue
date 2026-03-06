use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use std::time::Duration;

#[derive(Debug, Clone)]
pub enum BackoffStrategy {
    Fixed(Duration),
    Exponential { base: Duration, max: Duration },
}

impl BackoffStrategy {
    pub(crate) fn delay_for_attempt(&self, attempt: u32) -> Duration {
        match self {
            Self::Fixed(delay) => *delay,
            Self::Exponential { base, max } => {
                let base_ms = duration_to_millis(*base);
                let max_ms = duration_to_millis(*max);
                let exp = attempt.saturating_sub(1).min(20);
                let factor = 1u128 << exp;
                let delay_ms = base_ms.saturating_mul(factor).min(max_ms);
                Duration::from_millis(u64::try_from(delay_ms).unwrap_or(u64::MAX))
            }
        }
    }
}

impl Default for BackoffStrategy {
    fn default() -> Self {
        Self::Exponential {
            base: Duration::from_secs(1),
            max: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProcessorOptions {
    pub max_attempts: u32,
    pub lock_timeout: Duration,
    pub poll_interval: Duration,
    pub backoff: BackoffStrategy,
}

impl Default for ProcessorOptions {
    fn default() -> Self {
        Self {
            max_attempts: 20,
            lock_timeout: Duration::from_secs(300),
            poll_interval: Duration::from_millis(250),
            backoff: BackoffStrategy::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EnqueueOptions {
    pub delay: Duration,
    pub priority: i32,
}

impl Default for EnqueueOptions {
    fn default() -> Self {
        Self {
            delay: Duration::ZERO,
            priority: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum JobError {
    #[error("{0}")]
    Retryable(String),
    #[error("{0}")]
    Permanent(String),
}

impl JobError {
    pub fn retryable(message: impl Into<String>) -> Self {
        Self::Retryable(message.into())
    }

    pub fn permanent(message: impl Into<String>) -> Self {
        Self::Permanent(message.into())
    }

    pub(crate) fn is_retryable(&self) -> bool {
        matches!(self, Self::Retryable(_))
    }
}

#[async_trait]
pub trait Job: Serialize + DeserializeOwned + Send + Sync + 'static {
    async fn process(&self) -> Result<(), JobError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunOutcome {
    Idle,
    Completed {
        job_id: i64,
        attempts: u32,
    },
    Retried {
        job_id: i64,
        attempts: u32,
        next_run_at: i64,
        error: String,
    },
    Failed {
        job_id: i64,
        attempts: u32,
        error: String,
    },
}

fn duration_to_millis(duration: Duration) -> u128 {
    u128::from(duration.as_secs())
        .saturating_mul(1_000)
        .saturating_add(u128::from(duration.subsec_millis()))
}
