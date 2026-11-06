# Protocol implementation: A-01

The initial Rust implementation lives in [crates/protocol](../../crates/protocol/src/lib.rs) and derives numeric IDs from [protocol-registry.md](../architecture/protocol-registry.md). It uses a strict, byte-bounded frame codec and explicit `CodecError` values. A golden HELLO byte array and independently constructed Q1 PUB_ACK, Q2 PUB_ACK and NO_QUORUM error vectors are tested. HELLO minor/maximum-frame/required-optional capability negotiation is implemented. Remaining per-frame golden fixtures are added with their A-02 and later consumers; a binary registry is centrally defined rather than invented in each crate.

`decode_prefix` returns `Ok(None)` for an incomplete frame and parses only the first complete frame when multiple are present. The session task must enforce a maximum buffered prefix even if a client never completes a declared frame; this is **not** permission to accumulate arbitrary TCP bytes.

Fields unknown to major version 1 are skipped only if optional. Durable ACK and replay references must always carry log incarnation. The codec only validates syntax/required fields; it does not authenticate principals or make delivery/durability decisions. Those belong to A-02 and subsequent slices.

The present codec and reference wire registry passed fast Rust CI; end-to-end interoperability is **not** established until A-02 network sessions and complete frame fixtures are tested.
