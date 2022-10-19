use anyhow::Result;
use pay_tx::run;
use pay_tx::run_stream;

#[tokio::main]
async fn main() -> Result<()> {
    run_stream().await?;
    // run()?;
    Ok(())
}
