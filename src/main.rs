use anyhow::Result;
use csv::*;

fn main() -> Result<()> {
    println!("Hello, CSV world!");
    run()?;
    Ok(())
}
