use anyhow::{anyhow, bail, Result};
use csv::Reader;
use csv::StringRecord;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::env;

use futures::{stream, StreamExt};

use std::fs::File;
use std::io::{BufRead, BufReader};

use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;

// global variable

type Bal = HashMap<usize, Balance>;

pub async fn run_stream() -> Result<()> {
    let csv_file = input_filename()?;
    println!("csv_file {csv_file}");

    let file = File::open(csv_file)?;
    let reader = BufReader::new(file);
    let mut read_iter = reader.lines();
    // skip header
    read_iter.next();

    // convert iterator to stream.
    let stream = stream::iter(read_iter);

    let mut bal: Bal = HashMap::new();
    let mut tx_amt: HashMap<usize, f64> = HashMap::new();
    let mut dispute_txs: HashSet<usize> = HashSet::new();
    let buf_factor = 5;

    // create N RW_locks to distribute and reduce the write_lock wait time, here N = 256, u8 type.
    let mut inner = HashMap::new();
    for i in 0..=u8::max_value() {
        inner.insert(i, RwLock::new(HashMap::new()));
    }
    let rw_lock_map = Arc::new(inner);

    let res = stream
        .map(|line| get_tx(line.unwrap()) )
        .buffer_unordered(buf_factor)
        .map(|x| {
            let rw_lock_map = Arc::clone(&rw_lock_map);
            process_tx_async(x, &mut bal, &mut tx_amt, &mut dispute_txs, rw_lock_map)
                .unwrap_or(());
            async move { 0_usize }
        })
        .buffer_unordered(buf_factor)
        .collect::<Vec<_>>()
        .await;
    println!("stream res {res:?}");

    Ok(())
}

async fn get_tx(line: String) -> Result<Tx> {
    let items = line.split(',').collect::<Vec<&str>>();
    let mut record = StringRecord::from(items);
    record.trim();
    let tx: Tx = record.deserialize(None)?;
    Ok(tx)
}

fn process_tx_async(
    tx: Result<Tx>,
    bal: &mut Bal,
    tx_amt: &mut HashMap<usize, f64>,
    dispute_txs: &mut HashSet<usize>,
    rw_lock_map: Arc<HashMap<u8, RwLock<HashMap<usize, Mutex<usize>>>>>,
) -> Result<()> {
    let tx = &tx?;
    let client_id = tx.client;
    loop {
        let rw_lock_id = client_id as u8; // id % 256, map client_id to a rw_lock,
        let rw_lock = &rw_lock_map[&rw_lock_id]; // prefilled, must exists.
                                                 // Assume that the element already exists
        let client_lock = rw_lock.read().expect("RwLock poisoned");
        if let Some(data_lock) = client_lock.get(&client_id) {
            let mut _lock = data_lock.lock().expect("Mutex poisoned");

            match &tx.tx_type[..] {
                "deposit" => deposit(tx, bal, tx_amt)?,
                "withdrawal" => withdraw(tx, bal, tx_amt)?,
                "dispute" => dispute(tx, bal, tx_amt, dispute_txs)?,
                "resolve" => resolve(tx, bal, tx_amt, dispute_txs)?,
                "chargeback" => chargeback(tx, bal, tx_amt, dispute_txs)?,
                _ => (),
            }
            break;
        }
        drop(client_lock);
        let mut client_lock = rw_lock.write().expect("RwLock poisoned");

        // We use HashMap::entry to handle the case when another thread
        // inserted the same key, while it is unlocked.
        thread::sleep(Duration::from_millis(5));
        client_lock
            .entry(client_id)
            .or_insert_with(|| Mutex::new(0));
    }

    print_result(bal)?;
    Ok(())
}

pub fn run() -> Result<()> {
    let csv_file = input_filename()?;
    println!("csv_file {csv_file}");

    let txs = data_from_csv_trim(&csv_file)?;
    process_tx(&txs)?;

    Ok(())
}

fn process_tx(txs: &[Tx]) -> Result<()> {
    let mut bal: Bal = HashMap::new();
    let mut tx_amt: HashMap<usize, f64> = HashMap::new();
    let mut dispute_txs: HashSet<usize> = HashSet::new();

    for tx in txs {
        match &tx.tx_type[..] {
            "deposit" => deposit(tx, &mut bal, &mut tx_amt)?,
            "withdrawal" => withdraw(tx, &mut bal, &mut tx_amt)?,
            "dispute" => dispute(tx, &mut bal, &mut tx_amt, &mut dispute_txs)?,
            "resolve" => resolve(tx, &mut bal, &mut tx_amt, &mut dispute_txs)?,
            "chargeback" => chargeback(tx, &mut bal, &mut tx_amt, &mut dispute_txs)?,
            _ => (),
        }
    }

    // println!("tx_amt {tx_amt:?}");
    print_result(&bal)?;

    Ok(())
}

fn deposit(tx: &Tx, bal: &mut Bal, tx_amt: &mut HashMap<usize, f64>) -> Result<()> {
    println!("deposit {tx:?}");
    if tx_amt.contains_key(&tx.tx_id) {
        println!("TX {} already applied", tx.tx_id);
        return Ok(());
    }
    let client = tx.client;
    bal.entry(client).or_insert_with(|| {
        println!("Cleint {} not exists", client);
        Balance::new(client)
    });
    // let client_bal = bal.get_mut(&client).unwrap();
    // client_bal.available += tx.amount;
    let client_data = bal.entry(client).or_insert_with(|| Balance::new(client));
    if client_data.locked {
        println!("Cleint {} is locked, return", client);
        return Ok(());
    }
    client_data.available += tx.amount;
    tx_amt.insert(tx.tx_id, tx.amount);
    Ok(())
}

fn withdraw(tx: &Tx, bal: &mut Bal, tx_amt: &mut HashMap<usize, f64>) -> Result<()> {
    println!("withdraw {tx:?}");
    if tx_amt.contains_key(&tx.tx_id) {
        println!("TX {} already applied", tx.tx_id);
        return Ok(());
    }
    let client = tx.client;
    if !bal.contains_key(&client) {
        println!("Warning! Cleint {} not exists", client);
        return Ok(());
    }

    let client_data = bal.entry(client).or_insert_with(|| Balance::new(client));
    if client_data.locked {
        println!("Cleint {} is locked, return", client);
        return Ok(());
    }
    if client_data.available < tx.amount {
        println!("Warning! Cleint {} not enough balance", client);
        return Ok(());
    }
    client_data.available -= tx.amount;
    tx_amt.insert(tx.tx_id, -tx.amount);
    Ok(())
}

fn dispute(
    tx: &Tx,
    bal: &mut Bal,
    tx_amt: &mut HashMap<usize, f64>,
    dispute_txs: &mut HashSet<usize>,
) -> Result<()> {
    println!("dispute {tx:?}");
    let client = tx.client;
    if !bal.contains_key(&client) {
        println!("Warning! Cleint {} not exists", client);
        return Ok(());
    }
    if !tx_amt.contains_key(&tx.tx_id) {
        println!("Warning! tx {} not exists", tx.tx_id);
        return Ok(());
    }

    let client_data = bal.entry(client).or_insert_with(|| Balance::new(client));
    if client_data.locked {
        println!("Warning! Cleint {} is locked, return", client);
        return Ok(());
    }
    if dispute_txs.contains(&tx.tx_id) {
        println!("Warning! dispute tx {} already applied", tx.tx_id);
        return Ok(());
    }
    dispute_txs.insert(tx.tx_id);
    client_data.available -= tx_amt[&tx.tx_id];
    client_data.held += tx_amt[&tx.tx_id];
    Ok(())
}

fn resolve(
    tx: &Tx,
    bal: &mut Bal,
    tx_amt: &mut HashMap<usize, f64>,
    dispute_txs: &mut HashSet<usize>,
) -> Result<()> {
    println!("resolve {tx:?}");
    if !dispute_txs.contains(&tx.tx_id) {
        println!("Warning! dispute tx {} not exists", tx.tx_id);
        return Ok(());
    }

    let client = tx.client;
    if !bal.contains_key(&client) {
        println!("Warning! Cleint {} not exists", client);
        return Ok(());
    }
    let client_data = bal.entry(client).or_insert_with(|| Balance::new(client));
    if !tx_amt.contains_key(&tx.tx_id) {
        println!("Warning! tx {} not exists", tx.tx_id);
        return Ok(());
    }
    client_data.available += tx_amt[&tx.tx_id];
    client_data.held -= tx_amt[&tx.tx_id];
    dispute_txs.remove(&tx.tx_id);
    Ok(())
}

fn chargeback(
    tx: &Tx,
    bal: &mut Bal,
    tx_amt: &mut HashMap<usize, f64>,
    dispute_txs: &mut HashSet<usize>,
) -> Result<()> {
    println!("chargeback {tx:?}");
    if !dispute_txs.contains(&tx.tx_id) {
        println!("Warning! dispute tx {} not exists", tx.tx_id);
        return Ok(());
    }

    let client = tx.client;
    if !bal.contains_key(&client) {
        println!("Warning! Cleint {} not exists", client);
        return Ok(());
    }
    let client_data = bal.entry(client).or_insert_with(|| Balance::new(client));
    if !tx_amt.contains_key(&tx.tx_id) {
        println!("Warning! tx {} not exists", tx.tx_id);
        return Ok(());
    }
    client_data.held += tx_amt[&tx.tx_id];
    client_data.locked = true;
    Ok(())
}

fn _data_from_csv_no_space(csv_file: &str) -> Result<Vec<Tx>> {
    // csv file must be cleaned up, without spaces between fields.
    let mut rdr = Reader::from_path(csv_file)?;
    let res = rdr
        .deserialize()
        .map(|r| r.map_err(|e| anyhow!("{}", e)))
        .collect::<Result<Vec<Tx>>>();
    res
}

fn data_from_csv_trim(csv_file: &str) -> Result<Vec<Tx>> {
    // trim extra spacess before deserialize to struct data.
    let mut rdr = Reader::from_path(csv_file)?;
    let res = rdr
        .records() // yield the iterator is a Result<StringRecord, Error>
        .map(|r| {
            let mut record = r?; // r type is Result<StringRecord, Error>
            record.trim();
            record.deserialize(None) // -> Result<D> , D is the struct type Tx,
        })
        .map(|r| r.map_err(|e| anyhow!("csv read error: {e}")))
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

fn print_result(bal: &Bal) -> Result<()> {
    println!("client, available, held, total, locked");
    for client_bal in bal.values() {
        let Balance {
            client,
            available,
            held,
            locked,
        } = client_bal;
        let total = available + held;
        println!("{client}, {available}, {held}, {total}, {locked}");
    }
    Ok(())
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
    locked: bool,
}

impl Balance {
    fn new(client: usize) -> Self {
        Balance {
            client,
            available: 0.0,
            held: 0.0,
            locked: false,
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

#[cfg(test)]
mod tests;
