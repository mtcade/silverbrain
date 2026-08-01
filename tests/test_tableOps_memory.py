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

from pathlib import Path

import polars as pl
import pytest

from silverbrain.cell import Cell
from silverbrain.tableOps import MemoryOp, ReadParquetStaticOp, WriteParquetStaticOp

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


def _incr_or_escape( dfs: tuple, verbose: int = 0, verbose_prefix: str = '' ) -> tuple:
    """Loop controller: while n < 3, increment and loop (real -> 'feed',
    None -> 'escape'); once n == 3, stop looping and escape (None ->
    'feed', real -> 'escape') -- same shape as
    test_cell_nodes.py::_loop_controller_op."""
    held = dfs[ 0 ]
    n = held[ 'n' ][ 0 ]
    if n < 3:
        return ( held.with_columns( ( pl.col( 'n' ) + 1 ).alias( 'n' ) ), None )
    return ( None, held )


def test_read_once_mutate_in_memory_write_once( tmp_path: Path ) -> None:
    """Flagship integration test for the "memoryful table ops" design:
    a table is read from disk exactly once, mutated three times purely in
    memory via a loop, then written back to disk exactly once."""
    in_fp  = str( tmp_path / 'in.parquet' )
    out_fp = str( tmp_path / 'out.parquet' )
    pl.DataFrame( { 'n': [ 0 ] } ).write_parquet( in_fp )

    c = Cell(
        main_id='c',
        ops={
            'read':       ReadParquetStaticOp( columns=_COLUMNS, path=in_fp ),
            'mem':        MemoryOp( columns=_COLUMNS ),
            'controller': _incr_or_escape,
            'write':      WriteParquetStaticOp( columns=_COLUMNS, path=out_fp ),
        },
        source=[ 'trigger' ], target=[ 'written' ],
    )
    # 'read' (fired once, via the external trigger) and 'controller'
    # (fired repeatedly, via the loop) both target 'feed' -- target names
    # may repeat, the wiring graph isn't a DAG.
    c.register_node( 'read',       source=[ 'trigger' ], target=[ 'feed' ] )
    c.register_node( 'mem',        source=[ 'feed' ],    target=[ 'held' ] )
    c.register_node( 'controller', source=[ 'held' ],    target=[ 'feed', 'escape' ] )
    c.register_node( 'write',      source=[ 'escape' ],  target=[ 'written' ] )

    result = c( ( pl.DataFrame( { 'ignored': [ 1 ] } ), ) ).result( timeout=5 )
    assert result[ 0 ][ 'n' ].to_list() == [ 3 ]
    assert pl.read_parquet( out_fp )[ 'n' ].to_list() == [ 3 ]
