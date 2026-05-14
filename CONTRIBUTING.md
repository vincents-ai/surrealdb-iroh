# Contributing to surrealdb-iroh

Thank you for your interest in contributing!

## Ways to Contribute

- Report bugs and issues
- Suggest new features
- Submit bug fixes
- Improve documentation
- Add tests

## Development Setup

```bash
# Clone the repo
git clone https://github.com/vincents-ai/surrealdb-iroh.git
cd surrealdb-iroh

# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Run tests
cargo test

# Run clippy
cargo clippy -- -D warnings

# Format
cargo fmt
```

## Pull Request Process

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Make your changes
4. Run tests and clippy (`cargo test && cargo clippy -- -D warnings`)
5. Commit with clear messages
6. Push to your fork
7. Open a Pull Request
8. Sign the CLA when prompted (will appear as a PR check)

## Contributor License Agreement

Before we can accept your contributions, you must sign the [Individual CLA](https://gist.githubusercontent.com/shift/88a44f3d167eefd96e43a1ee22050ba0/raw/5aa0ff74c011f0a2e667f43a12e1836f9f507830/VINCENTS-AI_CLA.md).

This will be automatically requested when you open your first PR.

## License

By submitting a pull request, you agree that your contributions will be licensed under the Business Source License 1.1 (BSL-1.1).

For details, see [LICENSE](LICENSE).