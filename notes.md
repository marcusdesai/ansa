# Notes

## Profiling

Install [samply][samply-github] with:

```commandline
cargo install --locked samply
```

[samply-github]: https://github.com/mstange/samply/?tab=readme-ov-file

To build the example binary, run:

```commandline
cargo build --profile profiling --features "cache-padded tree-borrows" --example spsc
```

To profile, run:

```commandline
samply record ./target/profiling/examples/spsc
```

## Benchmarking

For a benchmark, `BENCH`, defined in `Cargo.toml`, run:

```commandline
cargo bench --bench BENCH --features="cache-padded tree-borrows"
```
