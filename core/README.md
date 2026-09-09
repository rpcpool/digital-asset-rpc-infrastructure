# das-core

Shared building blocks for the DAS binaries: database pool, metrics, the Solana RPC wrapper, the Plerkle messenger queue, and metadata JSON handling.

## Transaction serialization

`transaction_serialization.rs` holds a local copy of Plerkle's transaction serializer instead of calling `plerkle_serialization::serializer::seralize_encoded_transaction_with_status`.

The published `plerkle_serialization` crate is pinned to the Solana 3.x crate line. It cannot decode a transaction v1 body, because v1 uses wincode and not bincode, and its `match` on `VersionedMessage` has no `V1` arm. This workspace is on the Solana 4.x line, so the two sides no longer share a type.

The FlatBuffer schema also predates v1. Its `TransactionVersion` enum declares only `Legacy` (0) and `V0` (1). We write `TransactionVersion(2)` for v1. The value is safe on the wire because the generated verifier range-checks the field only as an `i8`. No code in this workspace reads the version field back, so the value affects other consumers of the stream, not us.

Drop this module when a `plerkle_serialization` release supports Solana 4.x and v1.
