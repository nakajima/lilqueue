# lilqueue-seaorm

SeaORM SQLite adapter for [`lilqueue`](https://crates.io/crates/lilqueue).

This crate provides `SeaOrmQueue`, which implements lilqueue's `JobQueue`,
`LockableQueue`, `RetryableQueue`, and dashboard data traits using a SeaORM
SQLite connection.

```rust
use lilqueue::{JobProcessor, ProcessorOptions};
use lilqueue_seaorm::{SeaOrmQueue, SeaOrmQueueOptions};

let db = sea_orm::Database::connect("sqlite://queue.db?mode=rwc").await?;
let queue = SeaOrmQueue::new(db, SeaOrmQueueOptions::default()).await?;
let processor = JobProcessor::<EmailJob, _>::new(queue.clone(), ProcessorOptions::default());
```

Keep this adapter separate from `lilqueue-seekwel`; SeaORM/sqlx and
Seekwel/rusqlite use different SQLite bindings and should not be linked into the
same package unless your dependency graph resolves that explicitly.
