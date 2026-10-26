use std::sync::Arc;

use datafusion::execution::disk_manager::DiskManagerBuilder;
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::*;
use datafusion_kql::SessionContextKqlExt;

#[tokio::main]
async fn main() -> datafusion_kql::Result<()> {
    let runtime = Arc::new(
        RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(GreedyMemoryPool::new(256 << 10)))
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_max_temp_directory_size(32 << 20),
            )
            .build()?,
    );
    let ctx = SessionContext::new_with_config_rt(
        SessionConfig::new()
            .with_target_partitions(4)
            .with_batch_size(1024),
        Arc::clone(&runtime),
    );
    let frame = ctx
        .kql(
            r#"
        let T=materialize(range x from 0 to 199999 step 1);
        union (T | where x<100000 | summarize Rows=count(),Total=sum(x)),
              (T | where x>=100000 | summarize Rows=count(),Total=sum(x))
        "#,
        )
        .await?;
    frame.clone().show().await?;
    let bytes = runtime.disk_manager.used_disk_space();
    assert!(
        bytes > 0,
        "the bounded runtime must force actual snapshot spilling"
    );
    println!("Shared snapshot: {bytes} temporary disk bytes");
    frame.clone().show().await?;
    assert_eq!(runtime.disk_manager.used_disk_space(), bytes);
    drop(frame);
    assert_eq!(runtime.disk_manager.used_disk_space(), 0);
    Ok(())
}
