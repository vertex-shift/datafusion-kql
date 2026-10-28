use datafusion::prelude::*;
use datafusion_kql::SessionContextKqlExt;

#[tokio::main]
async fn main() -> datafusion_kql::Result<()> {
    let ctx = SessionContext::new();
    ctx.kql(
        r#"
        print signal=dynamic([0,1,0,-1,0,1,0,-1])
        | extend (frequency_real,frequency_imag)=series_fft(signal)
        | extend (restored,imaginary)=series_ifft(frequency_real,frequency_imag)
        | project signal,frequency_real,frequency_imag,restored
        "#,
    )
    .await?
    .show()
    .await?;
    ctx.kql(
        r#"
        print samples=dynamic([1,3,5,7,9])
        | extend (RSquare,Slope)=series_fit_line(samples)
        | extend (Minimum,MinimumIndex,Maximum,MaximumIndex,Mean)=series_stats(samples)
        | project RSquare,Slope,Minimum,Maximum,Mean
        "#,
    )
    .await?
    .show()
    .await?;
    Ok(())
}
