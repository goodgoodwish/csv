# process transaction from CSV file
Rust CSV

There are 2 versions of main program.

run() is real time process.

run_stream() is asynce streaming .

# run

## to show log output,
RUST_LOG=debug cargo run -- transactions.csv

# Test


## Unit test
only did 2 unit test for demo by limited time. we can easily extend unit tests to cover all the functions.

### tx_from_line()
###

## Integration test
deposite

withdraw

dispute

resolve

chargeback
