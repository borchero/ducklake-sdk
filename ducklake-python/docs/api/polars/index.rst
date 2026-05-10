==================
Polars Integration
==================

The :mod:`ducklake.polars` module exposes functional helpers that mirror Polars'
own ``sink_*`` and ``scan_*`` APIs. Most users should prefer the methods on
:class:`~ducklake.Table` (e.g. :meth:`~ducklake.Table.sink_polars`,
:meth:`~ducklake.Table.scan_polars`), which call into these helpers internally.

.. currentmodule:: ducklake.polars
.. autosummary::
    :toctree: _gen/

    sink_ducklake
