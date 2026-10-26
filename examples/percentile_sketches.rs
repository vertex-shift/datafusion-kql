use datafusion::prelude::*;
use datafusion_kql::SessionContextKqlExt;

#[tokio::main]
async fn main() -> datafusion_kql::Result<()> {
    let ctx = SessionContext::new();
    ctx.kql(
        r#"
        range latency from 1 to 100000 step 1
        | extend shard=latency%8
        | summarize distribution=tdigest(latency) by shard
        | summarize distribution=tdigest_merge(distribution)
        | project percentiles=percentiles_array_tdigest(distribution,50,95,99),
            maximum=percentile_tdigest(distribution,100,typeof(long))
    "#,
    )
    .await?
    .show()
    .await?;
    ctx.kql(
        r#"
        datatable(latency:long,requests:long)[10,1,20,8,30,1]
        | summarize median=percentilew(latency,requests,50),
            exact=percentilew_exact(latency,requests,50)
    "#,
    )
    .await?
    .show()
    .await?;
    Ok(())
}
