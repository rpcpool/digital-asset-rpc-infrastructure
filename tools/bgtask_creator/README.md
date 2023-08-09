# Background Task Creator

This tool serves as a task generator for the 'tasks' table, primarily focusing on the indexing of off-chain metadata for assets lacking such metadata. It does not execute these tasks but merely facilitates their creation. These tasks are subsequently processed by the background indexer.

The tool avoids creating new tasks for those previously marked as 'failed'. To reattempt these tasks, a waiting period of an hour is required, or alternatively, the 'delete' option provided by this tool can be utilized. This option allows for the deletion and subsequent recreation of all tasks.

Please note that the tool's primary function is task creation and not task execution.

Run `cargo run` to run the tool and `cargo run -- help` to see the options. Ensure `INGESTER_DATABASE_CONFIG` is configured. For local testing, it could be for example `{listener_channel="backfill_item_added", url="postgres://ingest@localhost/das"}`

Supported options:

--include-url: Should be of the format `https://entities.nft.helium.io%`. Note the % option

## Examples

### Find a task

For a specific NFT (also works for cNFTs)

```
cargo run -- --mint 9xozKt1ZgKQqUvahDv1wi9eWuoNNEyR2uEiGRU8YmH87 find
```

### Create new tasks for assets without metadata

For a collection:

```
cargo run -- --collection 5PA96eCFHJSFPY9SWFeRJUHrpoNF5XZL6RrE1JADXhxf create
```

To force a re-index even if there is metadata:

```
cargo run -- --collection 5PA96eCFHJSFPY9SWFeRJUHrpoNF5XZL6RrE1JADXhxf --force-reindex create

```

Specific NFT (works also for cNFTs):

```
cargo run -- --mint 9xozKt1ZgKQqUvahDv1wi9eWuoNNEyR2uEiGRU8YmH87 create
```
