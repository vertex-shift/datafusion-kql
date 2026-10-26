use std::sync::Arc;

use datafusion::arrow::array::{ListArray, StringArray};
use datafusion::arrow::datatypes::Int64Type;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::TableReference;
use datafusion::prelude::*;
use datafusion_kql::SessionContextKqlExt;

#[tokio::main]
async fn main() -> datafusion_kql::Result<()> {
    let ctx = SessionContext::new();
    let latencies = ListArray::from_iter_primitive::<Int64Type, _, _>([
        Some(vec![Some(10), Some(20), Some(30)]),
        Some(vec![Some(100), Some(200)]),
    ]);
    ctx.register_batch(
        TableReference::bare("Events"),
        RecordBatch::try_from_iter(vec![
            (
                "Service",
                Arc::new(StringArray::from(vec!["api-v1", "worker-v2"])) as _,
            ),
            ("Latencies", Arc::new(latencies) as _),
        ])
        .map_err(datafusion::common::DataFusionError::from)?,
    )?;
    ctx.kql(
        r#"
        let threshold=0x14;
        table('Events')
        | extend Service=replace_regex(Service,@'-v\d+','')
        | mv-expand Latencies to typeof(long)
        | where Latencies>=threshold
        | summarize Requests=count(),Total=sum(Latencies) by Service
        | sort by Service asc
        | project Service,Requests,Total,Label=strcat(strrep('*',2),Service)
    "#,
    )
    .await?
    .show()
    .await?;
    Ok(())
}
