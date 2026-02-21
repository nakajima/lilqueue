# lilqueue

it's just a lil job queue.

jobs are plain `serde`-serializable structs that implement a single-method trait:

- `Job::process(&self) -> Result<(), JobError>`

## example

```rust
use async_trait::async_trait;
use lilqueue::{Job, JobError, ProcessorOptions, SqliteJobProcessor};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize)]
struct EmailJob {
    to: String,
    body: String,
}

#[async_trait]
impl Job for EmailJob {
    async fn process(&self) -> Result<(), JobError> {
        // do work here
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let processor =
        SqliteJobProcessor::<EmailJob>::connect_path("queue.db", ProcessorOptions::default())
            .await?;
    let worker = processor.spawn_worker();

    processor
        .enqueue(&EmailJob {
            to: "user@example.com".into(),
            body: "hello".into(),
        })
        .await?;

    tokio::time::sleep(Duration::from_millis(200)).await;
    worker.shutdown_and_wait().await;

    Ok(())
}
```

## axum dashboard

Mount the built-in dashboard router into your existing Axum app:

```rust
use axum::Router;
use lilqueue::{dashboard, ProcessorOptions, SqliteJobProcessor};

let processor = SqliteJobProcessor::<EmailJob>::connect_path("queue.db", ProcessorOptions::default())
    .await?;

let app = Router::new()
    .nest("/queue", dashboard::router(processor.db().clone()));
```

dashboard routes:

- `GET /queue/` (HTML overview)
- `GET /queue/api/stats` (JSON counters)
- `GET /queue/api/jobs?limit=50` (JSON recent jobs)
