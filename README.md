# process transaction from CSV file
There are 2 versions of main program.

run() is real time process.

run_stream() is asynce streaming .

# Run

## to show log output,
$> RUST_LOG=debug cargo run -- transactions.csv

# Error handling
use type Result and anyhow lib.

# Performance and concurrency

## Streaming
async parallel processing by streaming buffered(), buffer up stream items to at most n futures.
buffer_unordered() may generate bad state data.

Todo: multi thread. send transaction requests for same client to same thread may help improve the throughput.

## Concurrency
use RW lock for create a new client.
use Mutex to protect concurrent updates on same client.

In real world, it may use database row level lock to protect concurrent update.

# Test
only did 2 unit tests and 3 integration tests for demo, by limited time. we can easily extend unit tests to cover all the functions, and more integration tests to cover more use cases.

## Test plan
$> cargo test

## Unit test

### tx_from_line()
### withdraw()

## Integration test
todo: deposite

todo: withdraw

dispute

resolve

chargeback

# Tooling
cargo fmt

cargo test

cargo clippy
