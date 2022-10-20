use super::*;

#[test]
fn it_works() {
    assert_eq!(2 + 2, 4);
}

#[tokio::test]
async fn tx_from_line_test() -> Result<()> {
    let input = "deposit,2,2,2.0".to_owned();
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
