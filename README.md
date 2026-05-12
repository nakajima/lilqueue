# lilqueue

it's just a lil job queue runner.

jobs are plain `serde`-serializable structs that implement a single-method trait:

- `Job::process(&self) -> Result<(), JobError>`

storage is provided by the application. `lilqueue` does not depend on sqlite, sqlx,
rusqlite, seaorm, or seekwel.

## queue traits

Implement the capability traits your storage supports:

- `JobQueue`: enqueue serialized jobs
- `LockableQueue`: atomically claim and complete jobs
- `RetryableQueue`: retry or fail claimed jobs

The worker runner requires `RetryableQueue`. Lock tokens, leases, and database-specific
state should live inside the queue adapter's `Claim` type, not in job code.

## example

```rust
use async_trait::async_trait;
use lilqueue::{Job, JobError, JobProcessor, ProcessorOptions};
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
    let queue = MyQueue::new(); // your storage adapter, implements RetryableQueue
    let processor = JobProcessor::<EmailJob, _>::new(queue, ProcessorOptions::default());
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

## adapter crates

This repo includes separate adapter crates:

- `crates/lilqueue-seaorm`: `SeaOrmQueue` over a SeaORM SQLite connection
- `crates/lilqueue-seekwel`: `SeekwelQueue` over Seekwel's global connection

They are separate packages so SeaORM/sqlx and Seekwel/rusqlite do not have to be
linked into the same dependency graph. Publish `lilqueue` first, then publish
adapter crates after that version is available on crates.io.

SeaORM:

```rust
use lilqueue::{JobProcessor, ProcessorOptions};
use lilqueue_seaorm::{SeaOrmQueue, SeaOrmQueueOptions};

let db = sea_orm::Database::connect("sqlite://queue.db?mode=rwc").await?;
let queue = SeaOrmQueue::new(db, SeaOrmQueueOptions::default()).await?;
let processor = JobProcessor::<EmailJob, _>::new(queue.clone(), ProcessorOptions::default());
```

Seekwel:

```rust
use lilqueue::{JobProcessor, ProcessorOptions};
use lilqueue_seekwel::{SeekwelQueue, SeekwelQueueOptions};
use seekwel::connection::Connection;

Connection::file("queue.db")?;
let queue = SeekwelQueue::global(SeekwelQueueOptions::default())?;
let processor = JobProcessor::<EmailJob, _>::new(queue.clone(), ProcessorOptions::default());
```

## axum dashboard

The dashboard is storage-agnostic too. Provide a type that implements
`dashboard::DashboardData`:

```rust
use axum::Router;
use lilqueue::dashboard;

let app = Router::new()
    .nest("/queue", dashboard::router(my_dashboard_data));
```

dashboard routes:

- `GET /queue/` (HTML overview)
- `GET /queue/api/stats` (JSON counters)
- `GET /queue/api/jobs?limit=50` (JSON recent jobs)
