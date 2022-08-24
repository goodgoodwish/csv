use anyhow::{bail, Result};
use std::env;
use csv::Reader;
use serde::Deserialize;

pub fn run() -> Result<()> {
    let csv_file = input_filename()?;
    println!("csv_file {csv_file}");

    let txs = data_from_csv(&csv_file)?;
    process_tx(&txs)?;

    Ok(())
}

fn process_tx(txs: &[Tx]) -> Result<()> {
    for tx in txs {
        match &tx.tx_type[..] {
            "deposit" => deposit(&tx)?,
            "withdrawal" => withdraw(&tx)?,
            _ => (),
        }
    }

    Ok(())
}

fn deposit(tx: &Tx) -> Result<()> {
    println!("deposit {tx:?}");
    Ok(())
}

fn withdraw(tx: &Tx) -> Result<()> {
    println!("withdraw {tx:?}");
    Ok(())
}

fn data_from_csv(csv_file: &str) -> Result<Vec<Tx>> {
    let mut res: Vec<Tx> = vec![];
    let mut rdr = Reader::from_path(csv_file)?;
    for row in rdr.deserialize() {
        let tx: Tx = row?;
        println!("{tx:?}", );
        res.push(tx);
    }
    Ok(res)
}

fn input_filename() -> Result<String> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        bail!("not good arguments.\n\nUsage: command csv_file \n");
    }
    let csv_file = args[1].clone();
    Ok(csv_file)
}

#[derive(Debug, Deserialize, PartialEq)]
struct Tx {
    tx_type: String,
    client: usize,
    tx_id: usize,
    amount: f64,
}

fn rec_from_csv(csv_file: &str) -> Result<()> {
    let mut rdr = Reader::from_path(csv_file)?;
    for result in rdr.records() {
        let record = result?;
        println!("{:?}", record);
    }
    Ok(())
}
// pub fn go() -> Result<(), Box<dyn Error>> {
//     let contents = "abc";

//     Ok(())
// }
