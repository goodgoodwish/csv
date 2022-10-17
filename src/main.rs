use anyhow::Result;
use pay_tx::run;
use pay_tx::run_steam;

#[tokio::main]
async fn main() -> Result<()> {
    run_steam().await?;
    run()?;
    Ok(())
}
