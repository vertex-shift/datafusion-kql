use datafusion::prelude::*;
use datafusion_kql::SessionContextKqlExt;

#[tokio::main]
async fn main() -> datafusion_kql::Result<()> {
    SessionContext::new()
        .kql(
            r#"
        datatable(Service:string, Timestamp:datetime, Duration:real) [
            'api', datetime(2025-01-01T00:10:00Z), 10.0,
            'api', datetime(2025-01-01T02:20:00Z), 30.0,
            'worker', datetime(2025-01-01T00:05:00Z), 50.0,
            'worker', datetime(2025-01-01T02:05:00Z), 70.0
        ]
        | make-series Average=avg(Duration) default=double(null)
            on Timestamp from datetime(2025-01-01) to datetime(2025-01-01T03:00:00Z)
            step 1h by Service
        | extend Filled=series_fill_linear(Average)
        | extend Stats=series_stats_dynamic(Filled)
        | project Service, Filled=tostring(Filled), Mean=toreal(Stats.avg)
        | order by Service asc
    "#,
        )
        .await?
        .show()
        .await?;
    Ok(())
}
