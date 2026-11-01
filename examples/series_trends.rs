use datafusion::prelude::*;
use datafusion_kql::SessionContextKqlExt;

#[tokio::main]
async fn main() -> datafusion_kql::Result<()> {
    let ctx = SessionContext::new();
    ctx.kql(
        r#"
        print Samples=dynamic([-20,1,2,3,4,5,6,40])
        | extend Scores=series_outliers(Samples,'tukey')
        | project Samples,Scores
        "#,
    )
    .await?
    .show()
    .await?;
    ctx.kql(
        r#"
        print Samples=dynamic([1,3,5,7,20,19,18,17])
        | extend Fit=series_fit_2lines_dynamic(Samples)
        | extend (RSquare,Split,Variance,ResidualVariance,Fitted)=series_fit_2lines(Samples)
        | project RSquare,Split,Fitted,
            LeftSlope=toreal(Fit.left_slope),RightSlope=toreal(Fit.right_slope)
        "#,
    )
    .await?
    .show()
    .await?;
    Ok(())
}
