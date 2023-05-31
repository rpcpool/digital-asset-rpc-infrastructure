# Transaction Forwarder

## Send single transaction

```
cargo run -- \
 --redis-url 'redis://localhost:6379' \
 --rpc-url $RPC_URL \
 --max-retries 10 \
 --concurrency 10 \
 single --txn 65MtykBysKAofpvKMkGPYotxQYFRHM47g99iCs6B9ZxfAbBmHKeLi2LSUA8KUcm4qYsot2z9AB4uREuUuEQNw8HA
```

## Backfill tree locally

```
cargo run -- \
 --redis-url 'redis://localhost:6379' \
 --rpc-url $RPC_URL \
 --max-retries 10 \
 --concurrency 10 \
 address --address Cu61XHSkbasbvBc3atv5NUMz6C8FYmocNkH7mtjLFjR7
```

## Backfill tree against Dev/Prod

```
cargo run -- \
 --redis-url $REDIS_URL \
 --rpc-url $RPC_URL \
 --max-retries 10 \
 --concurrency 3 \
 address --address Cu61XHSkbasbvBc3atv5NUMz6C8FYmocNkH7mtjLFjR7
```

If you want to run against a range, you can use the `before` and/or `after` parameters. Example:

```
cargo run -- \
 --redis-url $REDIS_URL \
 --rpc-url $RPC_URL \
 --max-retries 10 \
 --concurrency 10 \
 --after '4DbGBhhcNRar1tL12VWciqAGUsZNaeom9iuWDbza7cE4d3VR9BbD5wkbnu44b4sDkjiqT14nPCxVLzRAqtjhkkWj' \
 address --address Cu61XHSkbasbvBc3atv5NUMz6C8FYmocNkH7mtjLFjR7
```

This will push all transactions that are newer than `4DbGBhhcNRar1tL12VWciqAGUsZNaeom9iuWDbza7cE4d3VR9BbD5wkbnu44b4sDkjiqT14nPCxVLzRAqtjhkkWj`.

If we want to ensure the transactions are sent in an order we want, ensure that concurrency is set to 1. When there's concurrency, the ordering is
not guranteed.
