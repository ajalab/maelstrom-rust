# maelstrom-rust

This repository showcases distributed system implementations designed to handle workloads defined by [Maelstrom](https://github.com/jepsen-io/maelstrom).
Its primary goal is to explore and compare various replication protocols under the [`lin-kv`](https://github.com/jepsen-io/maelstrom/blob/main/doc/workloads.md#workload-lin-kv) workload, including scenarios with faults injected by Maelstrom.

## Implementations

As of this writing, none of the implementations persist data.

### [lin-kv-single](./src/bin/lin-kv-single.rs)

A simple key-value store that does not replicate data to other nodes and therefore does not guarantee linearizability when run on more than one node.

### [lin-kv-raft-request-log](./src/bin/lin-kv-raft-request-log.rs)

A distributed key-value store that replicates data using the [Raft](https://raft.github.io/) consensus algorithm.

It replicates each complete client request in the log, allowing the current leader to respond when it applies every committed request, including requests originally accepted by a different leader.

### [lin-kv-raft-command-log](./src/bin/lin-kv-raft-command-log.rs)

A distributed key-value store that replicates data using the [Raft](https://raft.github.io/) consensus algorithm.

When a leader accepts a client request, it keeps a local per-request handler with the client routing and request information.
That handler sends the response after the request commits.

## Running with Maelstrom

Use Maelstrom to run the implementations against the `lin-kv` workload.
For example, the following runs a 30-second test of `lin-kv-raft-request-log` with five nodes while injecting network partition faults every second:

```shell
path/to/maelstrom test -w lin-kv --bin target/release/lin-kv-raft-request-log --time-limit 30 --node-count 5 --concurrency 2n --log-stderr --nemesis partition --nemesis-interval 1
```
