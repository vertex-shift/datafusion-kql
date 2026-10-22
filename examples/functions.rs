use datafusion::common::ScalarValue;
use datafusion::prelude::*;
use datafusion_kql::{QueryParameters, SessionContextKqlExt};

#[tokio::main]
async fn main() -> datafusion_kql::Result<()> {
    let ctx = SessionContext::new();
    let parameters = QueryParameters::from([("minimum".into(), ScalarValue::Int64(Some(100)))]);
    ctx.kql_with_parameters(
        r#"
        declare query_parameters(minimum:long=50);
        let SlowRequests = (T:(Service:string, Duration:long), threshold:long) {
            T | where Duration >= threshold
        };
        let Seconds = (milliseconds:long) { milliseconds / 1000.0 };
        datatable(Service:string, Duration:long) [
            'api', 80, 'api', 150, 'worker', 250
        ]
        | invoke SlowRequests(minimum)
        | project Service, DurationSeconds=Seconds(Duration)
        | sort by DurationSeconds desc
        "#,
        &parameters,
    )
    .await?
    .show()
    .await?;
    Ok(())
}
