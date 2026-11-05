use datafusion::prelude::*;
use datafusion_kql::SessionContextKqlExt;

#[tokio::main]
async fn main() -> datafusion_kql::Result<()> {
    let ctx = SessionContext::new();
    ctx.kql(
        r#"
        print Times=dynamic([0,1,4,5]), Samples=dynamic([3,7,43,73])
        | extend (RSquare,Coefficients,Variance,ResidualVariance,Fitted)=
            series_fit_poly(Samples,Times,2)
        | project RSquare,Coefficients,Variance,ResidualVariance,Fitted
        "#,
    )
    .await?
    .show()
    .await?;
    ctx.kql(
        r#"
        range x from -1.0 to 1.0 step 0.125
        | extend y=pow(x,5)-3.0*pow(x,3)+2.0*x+6.0
        | summarize X=make_list(x),Y=make_list(y)
        | extend (RSquare,Coefficients,Variance,ResidualVariance,Fitted)=
            series_fit_poly(Y,X,5)
        | project RSquare,Coefficients,Points=array_length(Fitted)
        "#,
    )
    .await?
    .show()
    .await?;
    Ok(())
}
