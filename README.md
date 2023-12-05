# Running Commands
## Normal Run
1. cargo run
## Test Run
1. cargo test
2. cargo test test_fn_name
3. cargo test test_mod_name::test_fn_name -- --exact
### Additional Useful Test Keywords
1. "-- --nocapture" is used to let the print statements show in the console as opposed to being redirected by default
2. "-- --nocapture --test-threads=1" to run with synchrnous print and single thread
## Formatting
1. cargo fmt
