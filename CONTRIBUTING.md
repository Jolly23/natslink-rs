# Contributing

## Ground rules

- The supervision constants (dial timeout, reconnect and rebuild backoff,
  heartbeat interval, stop grace, flush-on-stop bound) are deliberately not
  configurable. Changing one is a behaviour change for every user; open an
  issue with the failure mode you observed before sending a PR.
- Log messages are an interface: operators grep for them. Do not rename an
  existing message or field key; add new ones.
- Errors must never contain credentials. Any new error path that can carry a
  URL goes through `redact::redact_error` / `redact_url`, and gets a case in
  `tests/redaction.rs`.
- This crate is the Rust port of the Go module `github.com/Jolly23/natslink`.
  A behaviour change lands in both implementations, each written the way its
  runtime does it best; a feature that exists in only one of them is a bug.
  Keep `GO_REFERENCE_VERSION` and `Cargo.toml` `package.metadata.go-reference`
  pointing at the Go release the crate matches.
- No business logic and no dependencies beyond `async-nats`, `tokio`,
  `tokio-util`, `tracing`, `bytes`, `futures`, `thiserror`, `rand` and `rapidhash`
  in the runtime library. Test-only dependencies do not change this boundary.

## Running the tests

Unit tests need no server; the integration tests return early when
`NATS_TEST_URL` is unset:

```bash
make test            # cargo fmt --check, clippy -D warnings, cargo test
```

The maintenance scripts require Python 3.9 or newer. `make reference` checks
that `GO_REFERENCE_VERSION` agrees with `package.metadata.go-reference` and
that the crate and Go reference share a major and minor version. Patch
versions may differ.

`testdata/contract-v1.json` is shared with the Go implementation. It covers
FIFO deduplication, raw empty payloads, expiry, subscriber gate/dedup ordering,
custom keys and URL redaction through each library's actual code paths. Run:

```bash
make conformance GO_CONFORMANCE_DIR=/path/to/natslink
```

The runner requires byte-identical fixture files, then runs the Go and Rust
contract tests. CI obtains the Go checkout at the full commit ID recorded in
`.github/conformance-go.sha`. Updating that pin requires a public commit with
the matching fixture; it is separate from the published Go version recorded
in `Cargo.toml`. Existing broker integration tests remain necessary for
connection recovery, overload and shutdown; the offline contract suite does
not cover those behaviors.

The integration suite needs two local NATS servers and the `docker` CLI. The
restart / pause drills `docker stop` / `start` / `restart` the container named
by `NATS_TEST_CONTAINER`, so the containers must publish fixed ports:

```bash
make itest           # starts the two containers, runs everything, removes them
```

Manually:

```bash
docker run -d --name natslink-nats-1 -p 127.0.0.1:4222:4222 nats:latest --auth natslink-itest-token
docker run -d --name natslink-nats-2 -p 127.0.0.1:4223:4222 nats:latest --auth natslink-itest-token

NATS_TEST_URL='nats://natslink-itest-token@127.0.0.1:4222' \
NATS_TEST_URL2='nats://natslink-itest-token@127.0.0.1:4223' \
NATS_TEST_CONTAINER=natslink-nats-1 \
cargo test --locked -- --test-threads=4

docker rm -f natslink-nats-1 natslink-nats-2
```

| Variable | Used by |
|----------|---------|
| `NATS_TEST_URL` | every integration test |
| `NATS_TEST_URL2` | dual-server mirror tests |
| `NATS_TEST_CONTAINER` | the restart / stop drills (`docker` container name of `NATS_TEST_URL`) |
| `NATSLINK_ACL_DOCKER_TEST=1` | `tests/rejection.rs` (ignored by default; spawns its own container) |

Point these only at throw-away local servers. Tests that stop the shared
server take a write lock on a test-local `RwLock`; every other test takes the
read lock. A new test that forgets the read lock will be interrupted by a
restart drill when run in parallel.

## Releasing

1. Bump `version` in `Cargo.toml`, add a `## [X.Y.Z] - YYYY-MM-DD` section to
   `CHANGELOG.md`, run `make reference`, open a PR. Keep major and minor in
   step with Go; implementation-specific fixes can advance only this patch.
   When changing the Go reference, update `GO_REFERENCE_VERSION` and the
   entire `package.metadata.go-reference` record together.
2. After the merge, tag `vX.Y.Z` on `main`. The release workflow refuses a tag
   whose version does not match `Cargo.toml`, and uses the CHANGELOG section as
   the release body.
3. Consumers depend on the crate by git tag:
   `natslink = { git = "https://github.com/Jolly23/natslink-rs", tag = "vX.Y.Z" }`.
