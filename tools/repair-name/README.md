# Repair name and symbol

## The tool reads from the local redis and updates the name and symbol in prod database

The tool is just a stripped out version of the nft ingester so there's a bunch of unused code
which I didn't get the time to remove. When this tool is running, run the txn forwarder and acc forwarder
to update the assets.

## Set the following ENVs

INGESTER_RPC_CONFIG='{url="http://localhost:8899", commitment="finalized"}'
INGESTER_DATABASE_CONFIG='{listener_channel="backfill_item_added", url="postgres://prod-db-url"}'
INGESTER_ROLE=Ingester
INGESTER_MESSENGER_CONFIG='{messenger_type="Redis", connection_config={ redis_connection_str="redis://localhost:6379" } }

## Running the tool

Just do `cargo run`. This should have the tool listening on the local redis. Then run the `(txn/acc)_forwarder` for the assets which needs the name and symbol to be updated. Just ensure that the transactions are forwarded to local redis.
