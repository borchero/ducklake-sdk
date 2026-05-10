# Development

Thanks for deciding to work on `ducklake`! You can create a development environment with the following steps.

## Install Tooling

To work on the `ducklake` Python SDK you'll need to install:

- [`pixi`](https://pixi.sh/latest/) to manage the Python and tooling environment
- [`rustup`](https://rustup.rs/) to manage the Rust toolchain for compiling the native module

## Environment Installation

```bash
git clone https://github.com/borchero/ducklake-sdk
cd ducklake-sdk
rustup show
pixi install
```

Build and install the native module locally with:

```bash
pixi run install-py
```

## Running the Tests

Python tests:

```bash
pixi run test-py
```

Rust tests:

```bash
pixi run test-rs
```

## Linting

```bash
pixi run lint
```

## Documentation

We use [Sphinx](https://www.sphinx-doc.org/en/master/index.html) together with
[MyST](https://myst-parser.readthedocs.io/), and write user documentation in markdown. If you are not yet familiar with
this setup, the [MyST docs for Sphinx](https://myst-parser.readthedocs.io/en/latest/sphinx/intro.html) are a good
starting point.

When updating the documentation, you can compile a local build of the documentation and then open it in your web
browser using the commands below:

```bash
pixi run -e docs docs
open ducklake-python/docs/_build/html/index.html
```
