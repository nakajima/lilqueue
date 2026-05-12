# lilqueue-seekwel

Seekwel adapter for [`lilqueue`](https://crates.io/crates/lilqueue).

This crate provides `SeekwelQueue`, which implements lilqueue's `JobQueue`,
`LockableQueue`, `RetryableQueue`, and dashboard data traits using Seekwel's
global connection.

```rust
use lilqueue::{JobProcessor, ProcessorOptions};
use lilqueue_seekwel::{SeekwelQueue, SeekwelQueueOptions};
use seekwel::connection::Connection;

Connection::file("queue.db")?;
let queue = SeekwelQueue::global(SeekwelQueueOptions::default())?;
let processor = JobProcessor::<EmailJob, _>::new(queue.clone(), ProcessorOptions::default());
```

Keep this adapter separate from `lilqueue-seaorm`; Seekwel/rusqlite and
SeaORM/sqlx use different SQLite bindings and should not be linked into the same
package unless your dependency graph resolves that explicitly.
