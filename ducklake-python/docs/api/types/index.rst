====================
Schemas & Data Types
====================

.. currentmodule:: ducklake

Schemas
-------

.. autosummary::
    :toctree: _gen/
    :template: autosummary/class.rst

    Schema
    Column
    TableName
    TableMetadata

Primitive Data Types
--------------------

.. autosummary::
    :toctree: _gen/
    :template: autosummary/datatype.rst
    :nosignatures:

    DataType
    Boolean
    Int8
    Int16
    Int32
    Int64
    Int128
    UInt8
    UInt16
    UInt32
    UInt64
    UInt128
    Float32
    Float64
    Decimal
    Varchar
    Blob
    Json
    Uuid
    Date
    Time
    TimeTz
    Timestamp
    TimestampTz
    Interval

Nested Data Types
-----------------

.. autosummary::
    :toctree: _gen/
    :template: autosummary/datatype.rst
    :nosignatures:

    List
    Struct
    Map

Partitioning
------------

.. autosummary::
    :toctree: _gen/
    :template: autosummary/class.rst

    Partitioning
    PartitionColumn

Snapshots & Maintenance
-----------------------

.. autosummary::
    :toctree: _gen/
    :template: autosummary/class.rst

    SnapshotMetadata
    MaintenanceResult

Data Files
----------

.. autosummary::
    :toctree: _gen/
    :template: autosummary/class.rst

    WriteDataFile
    DataFileStatistics
    ColumnStats
    DeleteFile
    ScanDataFile
    ScanResult
