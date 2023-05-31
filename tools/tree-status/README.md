# Tree Utilities

## Log Config

SQL statement logs can be noisy. You can ignore them by running:

```
export RUST_LOG=info,sqlx::query=off,tree_status=trace
```

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

## Fix Tree

```
cargo run -- \
 --rpc-url $RPC_URL \
 --max-retries 10 \
 --concurrency 3 \
 fix-tree --pg-url $DB_URL --redis-url $REDIS_URL --tree $TREE
```
