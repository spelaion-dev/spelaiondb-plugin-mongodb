# SpelaionDB's MongoDB plugin — rules for agents

MongoDB for [SpelaionDB](https://github.com/spelaion-dev/spelaiondb): the engine's driver, and
a small program that serves it to SpelaionDB over its plugin protocol. It also serves the
servers that speak MongoDB's protocol, each checked against a real one (FerretDB so far).
Apache-2.0.

**The rules of SpelaionDB's repository apply here** (its `AGENTS.md`: all the code is written
by an agent, the compiler, the tests and CI are the only reviewer; everything in English;
sign-off; one task, one short branch; commit subjects that read well in a changelog). This file
says only what is different in this repository.

## What this is
- `src/lib.rs`: the driver. It implements `Driver` and `Connection` of `spelaion-driver-api`
  (SpelaionDB's ADR-005) over MongoDB's own Rust driver. **It is sent requests, never
  statements** (protocol 1.1, ADR-056 there): SpelaionDB's core reads what the user typed and
  turns each statement into `{"database": "…", "command": {…}}`, a command document in
  canonical Extended JSON. The driver checks that shape, runs the command exactly as it is,
  and refuses statement text. `find`, `aggregate`, `listCollections` and `listIndexes` are read
  as cursors, a batch at a time as the core gives credit; any other command's reply is one row,
  and `insert`, `update` and `delete` report their `n` as the rows affected.
- `src/results.rs`: documents as rows and columns, as ADR-056 point 3 says. `_id`, then the
  top-level fields of the first batch in the order first seen (at most 256), then `…`, always
  there. A column is typed only when every value of the first batch has one BSON type that maps
  exactly (`int` and `long` to integers, `double`, `bool`, `string`, `date`, `decimal`,
  `objectId` as its hex digits); otherwise relaxed Extended JSON. What a later document has no
  column for, or what does not fit its column, goes to `…`. **Nothing is ever dropped.**
- `src/bin/spelaiondb-plugin-mongodb.rs`: the program. It hands the driver to
  `spelaion_plugin_sdk::run` and nothing else.
- `tests/mongodb.rs`: the type zoo (every BSON type as a cell, its logical and native type and
  its text) and the rest against real servers. **A new type or reply shape is added there
  first.** `tests/conformance.rs`: SpelaionDB's conformance set through requests, against
  MongoDB and FerretDB, and `plugin.toml` checked against what the program says of itself.
- `testing/compose.yaml` and `ci/services.sh`: MongoDB 8.0 and FerretDB 2.
- `plugin.toml`: the protocol version (1.1), the engine and the lowest SpelaionDB.

## How it connects
- **Only to the address it is given, directly** (`directConnection`): never to the other
  members a replica set names, and never through DNS (`mongodb+srv` is not offered; the crate's
  DNS resolver is not compiled in). SpelaionDB's core opens every route (tunnels, proxies,
  Kubernetes); a plugin that found servers by itself would go around them.
- **Settings** (`connect`'s `options`) are only `authSource` (default `admin`),
  `authMechanism` (`SCRAM-SHA-1` or `SCRAM-SHA-256`) and `replicaSet` (the expected set's
  name, never permission to discover its other members); anything else is refused, after the core
  has refused it too.
- **TLS** with rustls: `disable` and `prefer` connect without TLS (MongoDB has no "if the
  server can"); `require` encrypts without checking the certificate; `verify-ca` and
  `verify-full` both check the certificate and the server's name (rustls in MongoDB's driver
  cannot skip the name). Through a route, `route.rs` uses MongoDB's supported SOCKS5 connector
  to keep the logical name for TLS while forwarding only to the core's fixed socket. The
  listener is loopback-only, authenticates with a random per-session token, accepts only
  CONNECT for the exact logical host and port, bounds its sockets and handshake time, and
  closes every socket when the session drops. It must never resolve a SOCKS destination or
  connect any address except the core's. Any change needs adversarial route tests and the
  real TLS replica-set test in `tests/mongodb.rs` (wrong name, authority and set included).
  A client certificate and its key must be one PEM file.
- **Reads and writes are never retried** by the driver (`retryReads`, `retryWrites` off):
  nothing the user ran is run again.
- A connection asking to be read-only is refused: MongoDB has no read-only mode in SpelaionDB
  yet (ADR-056 point 6), and the core never asks for one.
- **Cancel** drops the running request: its connection closes and the server ends the
  operation; a cursor is closed with it.

## A plugin is the wire, not the rules
SpelaionDB's core splits, classifies and guards statements, keeps secrets, opens tunnels and
writes the audit (ADR-041 there). This plugin connects, runs what it is sent, cancels and
introspects. So, and these are not negotiable (`docs/plugin-protocol.md` in SpelaionDB's
repository, "What a plugin must never do"):

- **Standard output is the protocol and nothing else.** Never `println!`. What must be said
  goes to standard error, a line at a time.
- **The password arrives in `connect` and is written nowhere**: not to a log, an error, a
  file or the environment.
- **It runs what it is sent.** It never decides whether a request is a read, and never changes
  one.
- **Nothing is run again** after a connection is lost.
- **It ends when its standard input closes** (the SDK does this).
- It reads no configuration file, keychain or store of SpelaionDB's, and opens no tunnel.
- **Nothing it runs on its own reads data**: the explorer's calls list databases, collections
  and indexes, never counts or samples documents.

## The contract comes from SpelaionDB, by tag
`spelaion-driver-api`, `spelaion-plugin-sdk` and, for the tests, `spelaion-plugin-host` are
git dependencies on a tag of SpelaionDB's repository (`plugin-sdk-vX.Y.Z`), all on the same
tag. They are not on crates.io.

- **A protocol change is made there first** (the new version, with its tests), then here: move
  the tag, adapt, release. SpelaionDB then moves its pin.
- Arrow must stay the version SpelaionDB uses: batches cross between the two unchanged.
- `plugin.toml`'s `protocol` is what the SDK of that tag speaks; `tests/conformance.rs` fails
  if they differ.

## Releases
Pushing a tag `vX.Y.Z` (the version in `Cargo.toml`) runs the release every plugin shares
(the template's repository, called by commit): the four platforms, each archive with its
SHA-256, and notes that carry the pin in the form SpelaionDB's `plugins.toml` wants. **Nothing
is signed and no secret lives here**: a release reaches users only when SpelaionDB's
repository pins its hashes in a pull request of its own, whose CI runs SpelaionDB's tests
through that exact build (ADR-042 there).

## Branches
`main` is protected: it changes only through pull requests whose CI is green, and cannot be
force-pushed or deleted. A pull request that touches a file in `.github/CODEOWNERS` waits for
an owner. The work is tracked in SpelaionDB's tracker (Plane, project `SDB`): write the item's
identifier in the pull request's description, never in a commit subject.

## Before calling something done
- `cargo fmt --all`, `cargo clippy --all-targets -- -D warnings`.
- The tests against real servers (without the variables they are skipped, and that does
  **not** count as tested): `ci/services.sh`, then
  `SPELAION_TEST_MONGO=127.0.0.1:27018 SPELAION_TEST_FERRETDB=127.0.0.1:27019 cargo test`.
- `cargo deny check` if dependencies change.
- A change to how values are laid out is checked against SpelaionDB too: build this program and
  run SpelaionDB's tests through it (`SPELAION_DEV_PLUGIN_MONGODB=<this program>` with a
  development build there).
