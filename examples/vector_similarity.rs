use datafusion::prelude::*;
use datafusion_kql::SessionContextKqlExt;

#[tokio::main]
async fn main() -> datafusion_kql::Result<()> {
    let ctx = SessionContext::new();
    ctx.kql(
        r#"
        datatable(Name:string,A:dynamic,B:dynamic)
        [
            'aligned',dynamic([1,2,3]),dynamic([2,4,6]),
            'opposite',dynamic([1,2,3]),dynamic([-2,-4,-6]),
            'unequal',dynamic([1,2,3]),dynamic([4,5]),
            'missing',dynamic([1,null,3]),dynamic([4,5,6])
        ]
        | project Name,Dot=series_dot_product(A,B),
            Cosine=series_cosine_similarity(A,B),
            Pearson=series_pearson_correlation(A,B)
        | sort by Name asc
    "#,
    )
    .await?
    .show()
    .await?;
    ctx.kql(
        r#"
        range x from 1 to 5 step 1
        | extend y=2*x+10
        | make-series X=avg(x),Y=avg(y) on x from 1 to 6 step 1
        | project Correlation=series_pearson_correlation(X,Y),
            Similarity=series_cosine_similarity(X,Y),
            BroadcastDot=series_dot_product(X,2)
    "#,
    )
    .await?
    .show()
    .await?;
    Ok(())
}
