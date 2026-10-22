use datafusion::prelude::*;
use datafusion_kql::SessionContextKqlExt;

#[tokio::main]
async fn main() -> datafusion_kql::Result<()> {
    SessionContext::new()
        .kql(
            r#"
        datatable(Device:string, Timestamp:datetime, Samples:dynamic) [
            'api', datetime(2025-01-01T00:00:00Z), dynamic([1,2]),
            'api', datetime(2025-01-01T01:00:00Z), dynamic([3,4]),
            'worker', datetime(2025-01-01T00:00:00Z), dynamic([5])
        ]
        | mv-apply Sample=Samples to typeof(long) on (summarize Total=sum(Sample))
        | partition hint.strategy=native by Device (
            sort by Timestamp asc
            | scan declare(Running:long=0) with (
                step accumulate: true => Running=accumulate.Running+Total;
            )
        )
        | project Device, Timestamp, Total, Running
        | sort by Device asc, Timestamp asc
    "#,
        )
        .await?
        .show()
        .await?;
    Ok(())
}
