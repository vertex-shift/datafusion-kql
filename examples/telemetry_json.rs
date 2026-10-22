use datafusion::prelude::*;
use datafusion_kql::SessionContextKqlExt;

#[tokio::main]
async fn main() -> datafusion_kql::Result<()> {
    let ctx = SessionContext::new();
    ctx.kql(
        r#"
        datatable(Raw:string) [
            '{"service":"api","latencies":[12,20,31]}',
            '{"service":"worker","latencies":[40,80]}',
            '{"service":"api","latencies":[17,25]}'
        ]
        | extend Payload=parse_json(Raw)
        | project Service=tostring(Payload.service), Latency=Payload.latencies
        | mv-expand Latency to typeof(long)
        | summarize Measurements=count(), AverageMs=avg(Latency), PeakMs=max(Latency)
            by Service
        | order by Service asc
    "#,
    )
    .await?
    .show()
    .await?;
    Ok(())
}
