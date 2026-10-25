use datafusion::prelude::*;
use datafusion_kql::SessionContextKqlExt;

#[tokio::main]
async fn main() -> datafusion_kql::Result<()> {
    let ctx = SessionContext::new();
    ctx.kql(
        r#"
        let Daily =
            datatable(Day:string,User:string)[
                'Monday','alice','Monday','bob','Monday','test',
                'Tuesday','bob','Tuesday','carol']
            | summarize Users=hll(User,2),Active=hll_if(User,User!='test',2) by Day;
        Daily
        | summarize Week=hll_merge(Users),Active=hll_merge(Active)
        | project UniqueUsers=dcount_hll(Week),ActiveUsers=dcount_hll(Active)
        "#,
    )
    .await?
    .show()
    .await?;
    Ok(())
}
