# Contributing

Contributions are welcome! This guide will help you get started.

## Development Setup

```bash
# Clone the repository
git clone https://github.com/takurot/mlprep.git
cd mlprep

# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install development dependencies
pip install -e ".[dev]"

# Install Rust toolchain (if not already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

## Running Tests

### Rust Tests

```bash
cargo test
```

### Python Tests

```bash
pytest tests/
```

## Code Style

### Rust

```bash
cargo fmt --all
cargo clippy
```

### Python

```bash
ruff check python/
ruff format python/
```

## Building

### Development Build

```bash
maturin develop
```

### Release Build

```bash
maturin build --release
```

## Pull Request Process

1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feature/my-feature`)
3. **Make** your changes
4. **Run** tests and linting
5. **Commit** with a descriptive message
6. **Push** to your fork
7. **Open** a Pull Request

## Commit Message Format

```
<type>: <description>

[optional body]
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation
- `style`: Formatting
- `refactor`: Code restructuring
- `test`: Adding tests
- `chore`: Maintenance

## Reporting Issues

Please include:
- mlprep version (`mlprep --version`)
- Python version (`python --version`)
- Operating system
- Minimal reproducible example
- Expected vs actual behavior

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
