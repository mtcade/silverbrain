#
#//  test_tableOps_memory.py
#//  silverbrain
#//

"""
Tests for MemoryOp: a purely in-memory table register with no disk/backend
access of its own. Reading and writing are separate ops elsewhere in the
graph -- MemoryOp only remembers the last value it was given (on the
TableOp instance itself, so it survives across separate Cell.__call__
invocations) and echoes it back.
"""

import polars as pl
import pytest

from silverbrain.tableOps import MemoryOp

_COLUMNS = { 'n': pl.Int64 }


def test_memory_op_requires_columns() -> None:
    with pytest.raises( TypeError ):
        MemoryOp()


def test_memory_op_remembers_and_echoes_last_value() -> None:
    op = MemoryOp( columns=_COLUMNS )

    first  = op( ( pl.DataFrame( { 'n': [ 1 ] } ), ) )
    assert first[ 0 ][ 'n' ].to_list() == [ 1 ]

    second = op( ( pl.DataFrame( { 'n': [ 2 ] } ), ) )
    assert second[ 0 ][ 'n' ].to_list() == [ 2 ]

    # State persists on the instance -- a third call with unrelated input
    # only changes what's remembered going forward; it never reverts to
    # an earlier value on its own.
    third = op( ( pl.DataFrame( { 'n': [ 3 ] } ), ) )
    assert third[ 0 ][ 'n' ].to_list() == [ 3 ]
    assert op._table[ 'n' ].to_list() == [ 3 ]

# The "read once, mutate in memory, write once" flagship integration test
# (test_read_once_mutate_in_memory_write_once) lives in
# silverflow/tests/test_tableOps_memory_backend.py -- it exercises
# ReadParquetStaticOp/WriteParquetStaticOp alongside MemoryOp.
