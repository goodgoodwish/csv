use super::*;

#[test]
fn it_works() {
    assert_eq!(2 + 2, 4);
}

#[tokio::test]
async fn tx_from_line_test() -> Result<()> {
    let input = "deposit, 2, 2, 2.0".to_owned();
    let res = tx_from_line(input).await?;
    let exp = Tx {
        tx_type: "deposit".to_owned(),
        client: 2,
        tx_id: 2,
        amount: 2.0,
    };
    assert_eq!(res, exp);
    Ok(())
}

#[test]
fn withdraw_test() -> Result<()> {
    let mut bal = HashMap::from([(
        2,
        Balance {
            client: 2,
            available: 10.0,
            held: 0.0,
            locked: false,
        },
    )]);
    let mut tx_amt = HashMap::new();
    let tx = Tx {
        tx_type: "withdraw".to_owned(),
        client: 1,
        tx_id: 1,
        amount: 8.8,
    };

    withdraw(&tx, &mut bal, &mut tx_amt)?;
    assert_eq!(bal[&2].available, 10.0);

    let tx = Tx {
        tx_type: "withdraw".to_owned(),
        client: 2,
        tx_id: 1,
        amount: 8.8,
    };
    debug!("tx {tx:?}");

    withdraw(&tx, &mut bal, &mut tx_amt)?;
    let exp = 1.2;
    let is_good = (bal[&2].available - exp).abs() < 0.0001;
    assert!(is_good);

    Ok(())
}

#[tokio::test]
async fn dispute_integration_test() -> Result<()> {
    let csv_file = "src/tests/dispute01.csv".to_owned();
    let mut bal = HashMap::new();
    let mut tx_amt = HashMap::new();
    let mut dispute_txs: HashSet<usize> = HashSet::new();

    process_file(csv_file, &mut bal, &mut tx_amt, &mut dispute_txs).await?;

    let expect_res = Balance {
        client: 2,
        available: 5.0,
        held: -2.2,
        locked: false,
    };

    assert_eq!(bal[&2], expect_res);

    Ok(())
}

#[tokio::test]
async fn resolve_integration_test() -> Result<()> {
    let csv_file = "src/tests/resolve01.csv".to_owned();
    let mut bal = HashMap::new();
    let mut tx_amt = HashMap::new();
    let mut dispute_txs: HashSet<usize> = HashSet::new();

    process_file(csv_file, &mut bal, &mut tx_amt, &mut dispute_txs).await?;

    let exp = -2.0;
    assert_eq!(bal[&2].held, exp);

    let csv_file = "src/tests/resolve02.csv".to_owned();
    let mut bal = HashMap::new();
    let mut tx_amt = HashMap::new();
    let mut dispute_txs: HashSet<usize> = HashSet::new();

    process_file(csv_file, &mut bal, &mut tx_amt, &mut dispute_txs).await?;

    assert_eq!(bal[&2].available, 2.0);
    assert_eq!(bal[&2].held, 5.0);
    assert_eq!(bal[&2].locked, false);

    Ok(())
}

#[tokio::test]
async fn chargeback_integration_test() -> Result<()> {
    let csv_file = "src/tests/chargeback01.csv".to_owned();
    let mut bal = HashMap::new();
    let mut tx_amt = HashMap::new();
    let mut dispute_txs: HashSet<usize> = HashSet::new();

    process_file(csv_file, &mut bal, &mut tx_amt, &mut dispute_txs).await?;

    assert_eq!(bal[&2].held, 0.0);
    assert_eq!(bal[&2].available, 4.0);
    assert_eq!(bal[&2].locked, true);

    Ok(())
}
