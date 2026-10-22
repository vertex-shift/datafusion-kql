use datafusion::prelude::*;
use datafusion_kql::SessionContextKqlExt;

#[tokio::main]
async fn main() -> datafusion_kql::Result<()> {
    let ctx = SessionContext::new();
    let frame = ctx
        .kql(
            r#"
        let minimum = 100;
        let requests = datatable(Service:string, Duration:long, Success:bool) [
            "api", 120, true,
            "api", 250, false,
            "worker", 80, true,
            "worker", 400, true
        ];
        requests
        | where Duration >= minimum
        | summarize Requests=count(), Failures=countif(not(Success)),
                    AverageMs=avg(Duration) by Service
        | order by AverageMs desc
        "#,
        )
        .await?;

    println!(
        "Native logical plan:\n{}\n",
        frame.logical_plan().display_indent()
    );
    frame.show().await?;
    Ok(())
}
