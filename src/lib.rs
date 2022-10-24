use anyhow::{anyhow, bail, Result};
use csv::Reader;
use csv::StringRecord;
use log::{debug, info, warn};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::env;

use futures::{stream, StreamExt};

use std::fs::File;
use std::io::{BufRead, BufReader};

use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;

type Bal = HashMap<usize, Balance>;
type ReadWriteLockMap = Arc<HashMap<u8, RwLock<HashMap<usize, Mutex<()>>>>>;

#[derive(Debug, Deserialize, PartialEq)]
struct Tx {
    tx_type: String,
    client: usize,
    tx_id: usize,
    amount: f64,
    // amount: Option<f64>,
}

#[derive(Debug, PartialEq)]
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

pub async fn run_stream() -> Result<()> {
    info!("starting up");

    let csv_file = input_filename()?;
    debug!("csv_file {csv_file}");

    let mut bal: Bal = HashMap::new();
    let mut tx_amt: HashMap<usize, f64> = HashMap::new();
    let mut dispute_txs: HashSet<usize> = HashSet::new();
    process_file(csv_file, &mut bal, &mut tx_amt, &mut dispute_txs).await?;
    print_result(&bal)?;

    Ok(())
}

async fn process_file(
    csv_file: String,
    bal: &mut Bal,
    tx_amt: &mut HashMap<usize, f64>,
    dispute_txs: &mut HashSet<usize>,
) -> Result<()> {
    let file = File::open(csv_file)?;
    let reader = BufReader::new(file);
    let mut read_iter = reader.lines();
    // skip header
    read_iter.next();

    // convert iterator to stream.
    let stream = stream::iter(read_iter);
    let buf_factor = 5;

    // create N RW_locks to distribute and reduce the write_lock wait time, here N = 256, u8 type.
    let mut inner = HashMap::new();
    for i in 0..=u8::max_value() {
        inner.insert(i, RwLock::new(HashMap::new()));
    }
    let rw_lock_map = Arc::new(inner);

    stream
        .map(|line| tx_from_line(line.unwrap()))
        .buffered(buf_factor)
        .map(|x| {
            let rw_lock_map = Arc::clone(&rw_lock_map);
            match process_tx_async(x, bal, tx_amt, dispute_txs, rw_lock_map) {
                Ok(()) => (),
                Err(e) => {
                    warn!("Error tx: {}", e); // log error into a database or file...etc.
                }
            }
            // return a future for buffered() input.
            async move { () }
        })
        .buffered(buf_factor)
        .collect::<Vec<_>>()
        .await;

    Ok(())
}

async fn tx_from_line(line: String) -> Result<Tx> {
    // data format: tx_type,client,"tx_id","amount"
    type Record = (String, usize, usize, Option<f64>);

    let items = line.split(',').collect::<Vec<&str>>();
    let mut record = StringRecord::from(items);
    record.trim();
    let rec: Record = record.deserialize(None)?;
    // debug!("rec {rec:?}");
    let tx = Tx {
        tx_type: rec.0,
        client: rec.1,
        tx_id: rec.2,
        amount: rec.3.unwrap_or(0.0),
    };
    // debug!("tx {tx:?}");
    Ok(tx)
}

fn process_tx_async(
    tx: Result<Tx>,
    bal: &mut Bal,
    tx_amt: &mut HashMap<usize, f64>,
    dispute_txs: &mut HashSet<usize>,
    rw_lock_map: ReadWriteLockMap,
) -> Result<()> {
    let tx = &tx?;
    let client_id = tx.client;
    loop {
        let rw_lock_id = client_id as u8; // id % 256, map client_id to a rw_lock,
        let rw_lock = &rw_lock_map[&rw_lock_id]; // prefilled, must exists.
        let client_read_lock = rw_lock.read().expect("RwLock poisoned");

        // Assume that the element <client_id here> already exists
        if let Some(data_lock) = client_read_lock.get(&client_id) {
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
        drop(client_read_lock);

        let mut client_write_lock = rw_lock.write().expect("RwLock poisoned");
        // We use HashMap::entry to handle the case when another thread
        // inserted the same key, while it is unlocked.
        thread::sleep(Duration::from_millis(5));
        client_write_lock
            .entry(client_id)
            .or_insert_with(|| Mutex::new(()));
    }

    Ok(())
}

pub fn run() -> Result<()> {
    let csv_file = input_filename()?;
    debug!("csv_file {csv_file}");

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

    println!("tx_amt {tx_amt:?}");
    print_result(&bal)?;

    Ok(())
}

fn deposit(tx: &Tx, bal: &mut Bal, tx_amt: &mut HashMap<usize, f64>) -> Result<()> {
    debug!("deposit {tx:?}");
    if !is_tx_valid(tx, bal, tx_amt) {
        return Ok(());
    }

    let client = tx.client;
    let client_data = bal.get_mut(&client).ok_or(anyhow!("bad client"))?;
    client_data.available += tx.amount;
    tx_amt.insert(tx.tx_id, tx.amount);
    Ok(())
}

fn withdraw(tx: &Tx, bal: &mut Bal, tx_amt: &mut HashMap<usize, f64>) -> Result<()> {
    debug!("withdraw {tx:?}");
    if !is_tx_valid(tx, bal, tx_amt) {
        return Ok(());
    }

    let client = tx.client;
    let client_data = bal.get_mut(&client).ok_or(anyhow!("bad client"))?;
    if client_data.available < tx.amount {
        warn!("Warning! Cleint {} not have enough balance", client);
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
    debug!("dispute {tx:?}");
    let client = tx.client;
    if !bal.contains_key(&client) {
        debug!("Warning! Cleint {} not exists", client);
        return Ok(());
    }
    if !tx_amt.contains_key(&tx.tx_id) {
        debug!("Warning! tx {} not exists", tx.tx_id);
        return Ok(());
    }

    let client_data = bal.entry(client).or_insert_with(|| Balance::new(client));
    if client_data.locked {
        debug!("Warning! Cleint {} is locked, return", client);
        return Ok(());
    }
    if dispute_txs.contains(&tx.tx_id) {
        debug!("Warning! dispute tx {} already applied", tx.tx_id);
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
    debug!("resolve {tx:?}");
    if !dispute_txs.contains(&tx.tx_id) {
        debug!("Warning! dispute tx {} not exists", tx.tx_id);
        return Ok(());
    }

    let client = tx.client;
    if !bal.contains_key(&client) {
        debug!("Warning! Cleint {} not exists", client);
        return Ok(());
    }
    let client_data = bal.entry(client).or_insert_with(|| Balance::new(client));
    if !tx_amt.contains_key(&tx.tx_id) {
        debug!("Warning! tx {} not exists", tx.tx_id);
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
    debug!("chargeback {tx:?}");
    if !dispute_txs.contains(&tx.tx_id) {
        debug!("Warning! dispute tx {} not exists", tx.tx_id);
        return Ok(());
    }

    let client = tx.client;
    if !bal.contains_key(&client) {
        debug!("Warning! Cleint {} not exists", client);
        return Ok(());
    }
    let client_data = bal.entry(client).or_insert_with(|| Balance::new(client));
    if !tx_amt.contains_key(&tx.tx_id) {
        debug!("Warning! tx {} not exists", tx.tx_id);
        return Ok(());
    }
    debug!("client_data {client_data:?}");
    debug!("tx_amt {tx_amt:?}");
    client_data.held -= tx_amt[&tx.tx_id];
    client_data.locked = true;
    Ok(())
}

fn is_tx_valid(tx: &Tx, bal: &mut Bal, tx_amt: &mut HashMap<usize, f64>) -> bool {
    if tx_amt.contains_key(&tx.tx_id) {
        debug!("TX {} already applied", tx.tx_id);
        return false;
    }
    let client = tx.client;
    if &tx.tx_type[..] == "deposit" {
        bal.entry(client).or_insert_with(|| {
            warn!("Cleint {} not exists", client);
            Balance::new(client)
        });
    }
    if !bal.contains_key(&client) {
        debug!("Warning! Cleint {} not exists", client);
        return false;
    }

    let client_data = &bal[&client];
    if client_data.locked {
        warn!("Cleint {} is locked, return", client);
        return false;
    }

    true
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

fn _rec_from_csv(csv_file: &str) -> Result<()> {
    let mut rdr = Reader::from_path(csv_file)?;
    for result in rdr.records() {
        let record = result?;
        debug!("{:?}", record);
    }
    Ok(())
}

fn _data_vec_from_csv(csv_file: &str) -> Result<Vec<Tx>> {
    let mut rdr = Reader::from_path(csv_file)?;
    let mut res: Vec<Tx> = vec![];
    for row in rdr.deserialize() {
        let tx: Tx = row?;
        debug!("{tx:?}",);
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
