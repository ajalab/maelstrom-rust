# maelstrom-rust

This repository showcases distributed system implementations designed to handle workloads defined by [Maelstrom](https://github.com/jepsen-io/maelstrom).
Its primary goal is to explore and compare various replication protocols under the [`lin-kv`](https://github.com/jepsen-io/maelstrom/blob/main/doc/workloads.md#workload-lin-kv) workload, including scenarios with faults injected by Maelstrom.

## Implementations

### [lin-kv-single](./src/bin/lin-kv-single.rs)

A simple key-value store that does not replicate data to other nodes and therefore does not guarantee linearizability when run on more than one node.
As of this writing, this implementation does not persist data.

### [lin-kv-raft](./src/bin/lin-kv-raft.rs)

A distributed key-value store that replicates data using the [Raft](https://raft.github.io/) consensus algorithm.
As of this writing, this implementation does not persist data.

## Running with Maelstrom

Use Maelstrom to run the implementations against the `lin-kv` workload.
For example, the following runs a 30-second test of `lin-kv-raft` with five nodes while injecting network partition faults every second:

```shell
path/to/maelstrom test -w lin-kv --bin target/release/lin-kv-raft --time-limit 30 --node-count 5 --concurrency 2n --log-stderr --nemesis partition --nemesis-interval 1
```
