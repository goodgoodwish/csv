# process transaction from CSV file
Rust CSV

There are 2 versions of main program.

run() is real time process.

run_stream() is asynce streaming .

# Run

## to show log output,
RUST_LOG=debug cargo run -- transactions.csv

# Error handling
use type Result and anyhow lib.

# Performance and concurrency

## Streaming
async parallel processing by streaming buffer_unordered.

## Concurrency
use RW lock for create a new client.
use Mutex to protect concurrent updates on same client.

In real world, it may use database row level lock to protect concurrent update.

# Test


## Unit test
only did 2 unit tests for demo by limited time. we can easily extend unit tests to cover all the functions.

### tx_from_line()
### withdraw()

## Integration test
deposite

withdraw

dispute

resolve

chargeback
