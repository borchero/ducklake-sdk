---
applyTo: ducklake-python/**
---

# DuckLake Python Package

This document describes the structure and core design principles for `ducklake-python`.

The `ducklake` Python package is a Python SDK for interacting with DuckLake. It wraps a native Rust extension (built
with PyO3/Maturin) to provide a Pythonic interface for managing DuckLake catalogs.

## Core Design Principles

### 1. Native Extension Wrapping Pattern

The package follows a **thin Python wrapper over native Rust** pattern:

- **Native classes** (`PyDucklake`, `PyTransaction`) are implemented in Rust and exposed via the `_native` module
- **Python wrapper classes** (`Ducklake`, `Transaction`) provide a Pythonic interface and additional functionality
- Wrapper classes use a **factory classmethod pattern** (`_from_pyducklake`, `_from_pytransaction`) to construct
  instances from native objects

```python
class Ducklake:
    _ducklake: PyDucklake  # Native object stored as private attribute

    @classmethod
    def _from_pyducklake(cls, pyducklake: PyDucklake, url: str) -> Ducklake:
        ducklake = cls.__new__(cls)
        ducklake._ducklake = pyducklake
        return ducklake
```

### 2. Entry Points

The package provides two main entry points in `connect.py`:

- `create(url, *, data_path)` - Initialize a new DuckLake catalog
- `connect(url)` - Connect to an existing DuckLake catalog

Both functions accept SQLAlchemy-compatible URLs and return a `Ducklake` instance.

### 3. Public API

All public symbols are explicitly exported in `__init__.py`:

```python
from .connect import connect, create
from .ducklake import Ducklake

__all__ = ["Ducklake", "connect", "create"]
```

Exceptions are re-exported in `exceptions.py` for explicit imports when needed.

### 4. Type Safety

- The package is fully typed with inline type annotations
- Native extension types are defined in `_native.pyi` stub file
- The `py.typed` marker indicates PEP 561 compliance

### 5. Optional Dependencies

The `_compat.py` module provides a compatibility layer for optional dependencies:

- Uses a `_DummyModule` class that raises helpful errors when an optional dependency is accessed but not installed

### 6. Transaction Context Manager

The `Transaction` class implements the context manager protocol:

- Automatically commits on successful exit
- Does not catch exceptions (allows them to propagate)

```python
with ducklake.transaction() as tx:
    tx.create_schema("my_schema")
    # Auto-commits on exit
```

### 7. Error Handling

- Native Rust errors are converted to Python exceptions in `error.rs`
- Custom exception types (`NotInitializedError`, `AlreadyInitializedError`) are defined in Rust and re-exported in
  Python

## Code Style Guidelines

### Python Code

- Use `from __future__ import annotations` for postponed annotation evaluation
- Private attributes and methods are prefixed with underscore (`_ducklake`, `_from_pyducklake`)
- Use keyword-only arguments for optional parameters (`*, data_path=None`)
- Include comprehensive docstrings with Args, Returns, and Raises sections

### Rust Code

- Use the `pyo3` crate for Python bindings
- Prefix Python-exposed classes with `Py` (`PyDucklake`, `PyTransaction`)
- Use the shared async runtime from `runtime.rs` for blocking on async operations
- Convert Rust errors to Python exceptions using the `error::into_pyerr` helper
