---
name: maelstrom
description: Evaluate a distributed node binary with any Maelstrom workload. Use when running or analyzing Maelstrom correctness tests, including optional network-partition failure tests.
---

# Evaluate Maelstrom workloads

Require the node binary path. Infer the workload from the request and state it; ask only if unclear.

## Workloads

- `echo`: Echo a payload.
- `unique-ids`: Generate globally unique IDs.
- `broadcast`: Eventually propagate messages.
- `g-counter`: Increment-only counter.
- `pn-counter`: Increment/decrement counter.
- `g-set`: Grow-only set.
- `lin-kv`: Linearizable KV: read, write, CAS.
- `kafka`: Append-only logs and offsets.
- `txn-list-append`: Transactional list reads and appends.
- `txn-rw-register`: Transactional register reads and writes.

## Prepare

Use `MAELSTROM_DIR` as the directory containing `maelstrom`. If unset, ask the user to set it:

```sh
export MAELSTROM_DIR="$HOME/path/to/maelstrom"
```

Verify Maelstrom and the binary are executable. Do not build the binary.

## Run

Default: 5 nodes, `2n` concurrency, 30 seconds. Omit nemesis flags unless requested.

```sh
"$MAELSTROM_DIR/maelstrom" test -w <workload> --bin <binary> \
  --time-limit 30 --node-count 5 --concurrency 2n
```

Honor explicit options.

## Failures

Add a partition nemesis only on request:

```sh
--nemesis partition --nemesis-interval <seconds>
```

## Assess

Report `:valid?`. On failure, give the results directory and inspect available results and node stderr.
