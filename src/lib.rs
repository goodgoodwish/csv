use anyhow::{anyhow, bail, Result};
use csv::Reader;
use serde::Deserialize;
use std::env;
use std::collections::HashMap;

pub fn run() -> Result<()> {
    let csv_file = input_filename()?;
    println!("csv_file {csv_file}");

    let txs = data_from_csv(&csv_file)?;
    process_tx(&txs)?;

    Ok(())
}

fn process_tx(txs: &[Tx]) -> Result<()> {
    let mut bal: HashMap<usize, Balance> = HashMap::new();

    for tx in txs {
        match &tx.tx_type[..] {
            "deposit" => deposit(&tx, &mut bal)?,
            "withdrawal" => withdraw(&tx, &mut bal)?,
            _ => (),
        }
    }

    println!("bal {bal:?}");

    Ok(())
}

fn deposit(tx: &Tx, bal: &mut HashMap<usize, Balance>) -> Result<()> {
    println!("deposit {tx:?}");
    let client = tx.client;
    if !bal.contains_key(&client) {
        println!("Cleint {} not exists", client);
        bal.insert(client, Balance::new(client));
    }
    // let client_bal = bal.get_mut(&client).unwrap();
    // client_bal.available += tx.amount;
    let client_data = bal.entry(client).or_insert(Balance::new(client));
    client_data.available += tx.amount;
    Ok(())
}

fn withdraw(tx: &Tx, bal: &mut HashMap<usize, Balance>) -> Result<()> {
    println!("withdraw {tx:?}");
    Ok(())
}

fn data_from_csv(csv_file: &str) -> Result<Vec<Tx>> {
    let mut rdr = Reader::from_path(csv_file)?;
    let res = rdr
        .deserialize()
        .map(|r| r.map_err(|e| anyhow!("{}", e)))
        .collect::<Result<Vec<Tx>>>();
    res
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

#[derive(Debug)]
struct Balance {
    client: usize,
    available: f64,
    held: f64,
}

impl Balance {
    fn new(client: usize) -> Self {
        Balance {
            client,
            available: 0.0,
            held: 0.0,
        }
    }
}

fn _rec_from_csv(csv_file: &str) -> Result<()> {
    let mut rdr = Reader::from_path(csv_file)?;
    for result in rdr.records() {
        let record = result?;
        println!("{:?}", record);
    }
    Ok(())
}

fn _data_vec_from_csv(csv_file: &str) -> Result<Vec<Tx>> {
    let mut rdr = Reader::from_path(csv_file)?;
    let mut res: Vec<Tx> = vec![];
    for row in rdr.deserialize() {
        let tx: Tx = row?;
        println!("{tx:?}",);
        res.push(tx);
    }
    Ok(res)
}

// pub fn go() -> Result<(), Box<dyn Error>> {
//     let contents = "abc";

//     Ok(())
// }
