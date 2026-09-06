# Publishing the standalone crate

This directory is the complete crate and can be published directly. The parent
Bun project is not a dependency. The included CI workflow expects this directory
to be the repository root when hosted in a dedicated repository.

1. If a source repository is available, add its actual URL as `repository` in
   `Cargo.toml`. Do not publish placeholder repository links.
2. Confirm the package name and version in `Cargo.toml`. For a first release,
   verify the name is available; for later releases, choose a new version and
   confirm the authenticated account has publish access.
3. Keep `LICENSE` and review the release notes in `CHANGELOG.md`. Check the
   session, capacity, refresh, and maintenance semantics in `README.md` and keep
   its dependency example consistent with the version being published.
4. From this directory, run:

   ```sh
   cargo fmt --check
   cargo clippy --locked --all-targets -- -D warnings
   cargo test --locked --all-targets
   cargo test --locked --doc
   cargo package --locked --list
   cargo publish --registry crates-io --locked --dry-run
   ```

5. Inspect the packaged README, example, and source together so the release
   documents the same API it contains. If using a source repository, commit the
   crate and CI workflow before publishing.
6. Authenticate to crates.io using your own account (`cargo login --registry crates-io`), or configure
   its supported trusted publishing flow for the actual destination repository.
7. Publish the reviewed artifact with `cargo publish --registry crates-io --locked`.
   If using a source repository, tag the matching commit with the published
   version and create its release.

The manifest uses an explicit file allowlist. Inspect the package list before
release: it should contain only this crate's sources, tests, example, lockfile,
license, and documentation. Runtime subscription files and proxy caches from
the parent project are outside the package.

`cargo publish --dry-run` prepares and verifies a package without uploading it.
The provided CI runs validation only; it does not publish automatically.

The test suite simulates subscriptions, conditional HTTP responses, proxy
failures, rotation, session isolation, and capacity locally. A successful dry
run validates packaging and compilation; it is not a live subscription test or
a performance benchmark.

The explicit registry flag also works when the local Cargo configuration uses
a crates.io mirror for downloading dependencies.
