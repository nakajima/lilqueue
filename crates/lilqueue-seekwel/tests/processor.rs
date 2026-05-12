use async_trait::async_trait;
use lilqueue::{BackoffStrategy, Job, JobError, JobProcessor, ProcessorOptions};
use lilqueue_seekwel::{SeekwelQueue, SeekwelQueueOptions};
use seekwel::connection::Connection;
use std::time::Duration;
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

#[tokio::test(flavor = "multi_thread")]
async fn seekwel_queue_processes_job() {
    let _ = Connection::memory();
    let queue = SeekwelQueue::global(SeekwelQueueOptions::default()).unwrap();
    let processor = JobProcessor::<WriteFileJob, _>::new(queue, test_options());
    let worker = processor.spawn_worker();

    let dir = tempdir().unwrap();
    let output_path = dir.path().join("output.log");
    processor
        .enqueue(&WriteFileJob {
            output_path: output_path.to_string_lossy().to_string(),
            line: "seekwel".to_string(),
        })
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if std::fs::read_to_string(&output_path)
                .map(|contents| contents.contains("seekwel"))
                .unwrap_or(false)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    worker.shutdown_and_wait().await;
}

fn test_options() -> ProcessorOptions {
    ProcessorOptions {
        max_attempts: 3,
        poll_interval: Duration::from_millis(1),
        backoff: BackoffStrategy::Fixed(Duration::ZERO),
    }
}
