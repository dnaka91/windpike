# ⛰️ Windpike

[![Build Status][build-img]][build-url]
[![Repository][crates-img]][crates-url]
[![Documentation][doc-img]][doc-url]

[build-img]: https://img.shields.io/github/actions/workflow/status/dnaka91/windpike/ci.yml?branch=legacy&style=for-the-badge
[build-url]: https://github.com/dnaka91/windpike/actions/workflows/ci.yml
[crates-img]: https://img.shields.io/crates/v/windpike?style=for-the-badge
[crates-url]: https://crates.io/crates/windpike
[doc-img]: https://img.shields.io/badge/docs.rs-windpike-4d76ae?style=for-the-badge
[doc-url]: https://docs.rs/windpike

A simple async [Aerospike](https://www.aerospike.com) client for Rust.

## Usage

Add the crate to your project with `cargo add`:

```sh
cargo add windpike
```

## Tests

The crate contains various integrations tests, which require a running Aerospike server instance to function. One can be quickly set up with [Podman](https://podman.io) or [Docker](https://www.docker.com) as follows (just replace `podman` with `docker` if you use Docker instead):

```sh
podman run --name aerospike -p 3000:3000 -d docker.io/aerospike/aerospike-server
```

Then, the tests can be run with `cargo test` as usual.

If needed, logs can be enabled with the `RUST_LOG` environment variable:

```sh
RUST_LOG=windpike=trace cargo test
```

## License

This project is licensed under [MIT License](LICENSE) (or <http://opensource.org/licenses/MIT>).
