# Performance Analysis

First, let's install tools and get them running.

## Profiling

On macOS and Linux, assuming you have firefox installed, [samply][samply-github] works well and can be installed with:

```commandline
cargo install --locked samply
```

[samply-github]: https://github.com/mstange/samply/?tab=readme-ov-file

We need to run a binary for samply to profile, and there are binaries in the `examples/` dir written for this purpose.
To build the example `spsc` binary, run:

```commandline
cargo build --profile profiling --features "cache-padded" --example spsc
```

Then to profile, run:

```commandline
samply record ./target/profiling/examples/spsc
```

Which will automatically open a report in firefox, using the firefox profiler.

## Benchmarking

For a benchmark, `BENCH`, defined in `Cargo.toml` (such as `spsc`), run:

```commandline
cargo bench --bench BENCH --features="cache-padded"
```

Or optionally, install `cargo-criterion`:

```commandline
cargo install cargo-criterion 
```

And instead run:

```commandline
cargo criterion --bench BENCH --features="cache-padded"
```
