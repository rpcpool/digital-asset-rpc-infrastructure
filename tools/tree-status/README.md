# Tree Utilities

## Tree Status

### Local

```
cargo run -- \
 --rpc-url $RPC_URL \
 --max-retries 10 \
 --concurrency 3 \
 check-tree --pg-url postgres://ingest@localhost/das --tree $TREE
```

### Remote

```
cargo run -- \
 --rpc-url $RPC_URL \
 --max-retries 10 \
 --concurrency 3 \
 check-tree --pg-url $DB_URL --tree $TREE
```
