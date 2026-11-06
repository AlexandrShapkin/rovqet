# Protocol implementation: A-01

The initial Rust implementation lives in [crates/protocol](../../crates/protocol/src/lib.rs) and derives numeric IDs from [protocol-registry.md](../architecture/protocol-registry.md). It uses a strict, byte-bounded frame codec and explicit `CodecError` values. The first checked-in golden HELLO fixture is expressed as a byte array in the protocol unit tests; additional frame/error golden fixtures and transport negotiation tests remain part of the A-01→A-02 work.

`decode_prefix` returns `Ok(None)` for an incomplete frame and parses only the first complete frame when multiple are present. The session task must enforce a maximum buffered prefix even if a client never completes a declared frame; this is **not** permission to accumulate arbitrary TCP bytes.

Fields unknown to major version 1 are skipped only if optional. Durable ACK and replay references must always carry log incarnation. The codec only validates syntax/required fields; it does not authenticate principals or make delivery/durability decisions. Those belong to A-02 and subsequent slices.

This implementation should not be treated as a frozen interoperability contract until it passes CI and the complete golden registry is committed.
