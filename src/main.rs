use anyhow::Result;
use pay_tx::run;

fn main() -> Result<()> {
    // println!("Hello, CSV world!");
    run()?;
    Ok(())
}
