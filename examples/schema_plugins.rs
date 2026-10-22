use datafusion::prelude::*;
use datafusion_kql::SessionContextKqlExt;

#[tokio::main]
async fn main() -> datafusion_kql::Result<()> {
    SessionContext::new()
        .kql(
            r#"
        datatable(Payload:dynamic) [
            dynamic({"Service":"api","Status":"ok","Value":10}),
            dynamic({"Service":"api","Status":"error","Value":2}),
            dynamic({"Service":"worker","Status":"ok","Value":20})
        ]
        | evaluate bag_unpack(Payload): (Service:string, Status:string, Value:long)
        | evaluate pivot(Status, sum(Value), Service): (Service:string, ok:long, error:long)
        | sort by Service asc
    "#,
        )
        .await?
        .show()
        .await?;
    Ok(())
}
