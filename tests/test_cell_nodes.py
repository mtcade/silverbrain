#
#//  test_cell_nodes.py
#//  silverbrain
#//

"""
Tests for Cell's declarative wiring engine (squishy-sleeping-snowflake.md):
__call__ seeds a call-scoped resolve of named values from self.source, runs
whichever registered __nodes__ entry has all its declared source names
available, feeding results forward by target name, until self.target is
fully resolved -- returning a concurrent.futures.Future, never blocking the
caller. register_node is the declarative-bookkeeping step that makes a
tableOps entry participate in this wiring.
"""

import polars as pl
import pytest

from silverbrain.cell import Cell
from silverbrain.parameterParse import ParameterParser
from silverbrain.tableOps import BackendReadOp, BackendWriteOp
from silverbrain.tableProcesses import backend_read_process, backend_write_process


def _double_op( dfs: tuple, verbose: int = 0, verbose_prefix: str = '' ) -> tuple:
    return ( dfs[ 0 ].with_columns( ( pl.col( 'v' ) * 2 ).alias( 'v' ) ), )


def _incr_op( dfs: tuple, verbose: int = 0, verbose_prefix: str = '' ) -> tuple:
    return ( dfs[ 0 ].with_columns( ( pl.col( 'v' ) + 1 ).alias( 'v' ) ), )


# -- __call__ / bind: async wiring dispatch


def test_call_resolves_a_single_wired_node() -> None:
    c = Cell( main_id='c', ops={ 'double': _double_op }, source=[ 'inp' ], target=[ 'out' ] )
    c.register_node( 'double', source=[ 'inp' ], target=[ 'out' ] )
    df_in = pl.DataFrame( { 'v': [ 1, 2, 3 ] } )
    result = c( ( df_in, ) ).result( timeout=5 )
    assert result[ 0 ][ 'v' ].to_list() == [ 2, 4, 6 ]


def test_call_wrong_arity_raises_valueerror() -> None:
    c = Cell( main_id='c', source=[ 'a', 'b' ] )
    with pytest.raises( ValueError ):
        c( ( pl.DataFrame( { 'v': [ 1 ] } ), ) )


def test_call_unresolvable_target_raises_runtimeerror() -> None:
    c = Cell( main_id='c', source=[ 'inp' ], target=[ 'never_produced' ] )
    fut = c( ( pl.DataFrame( { 'v': [ 1 ] } ), ) )
    with pytest.raises( RuntimeError ):
        fut.result( timeout=5 )


def test_nested_cell_dispatches_through_bind() -> None:
    child  = Cell( main_id='child', ops={ 'double': _double_op }, source=[ 'inp' ], target=[ 'out' ] )
    child.register_node( 'double', source=[ 'inp' ], target=[ 'out' ] )
    parent = Cell( main_id='parent', ops={ 'kid': child }, source=[ 'inp' ], target=[ 'out' ] )
    parent.register_node( 'kid', source=[ 'inp' ], target=[ 'out' ] )

    df_in = pl.DataFrame( { 'v': [ 5 ] } )
    result = parent( ( df_in, ) ).result( timeout=5 )
    assert result[ 0 ][ 'v' ].to_list() == [ 10 ]
    assert parent.bind( 'kid' ) is not child  # bind() always returns a wrapper, not the raw child
    assert parent.bind( 'kid' )( ( df_in, ) ).result( timeout=5 )[ 0 ][ 'v' ].to_list() == [ 10 ]


# -- __table_ops_parameters__: TableOp construction-parameter forest, always
# empty for now (no parameter sources exist yet) -- see configTree.py.


def test_cell_seeds_empty_parameters_table_by_default() -> None:
    c = Cell( main_id='c' )
    params = c.tables[ '__table_ops_parameters__' ]
    assert params.is_empty()
    assert params.schema == ParameterParser().schema


def test_cell_accepts_supplied_parameters() -> None:
    supplied = pl.DataFrame( schema = ParameterParser().schema )
    c = Cell( main_id='c', parameters=supplied )
    assert c.tables[ '__table_ops_parameters__' ] is supplied


def test_to_dataframes_includes_parameters() -> None:
    c = Cell( main_id='c' )
    snapshot = c.to_dataframes()
    assert 'parameters' in snapshot
    assert snapshot[ 'parameters' ].is_empty()


def test_to_dataframes_includes_source_and_target() -> None:
    c = Cell( main_id='c', source=[ 'inp' ], target=[ 'out' ] )
    snapshot = c.to_dataframes()
    assert snapshot[ 'source' ] == [ 'inp' ]
    assert snapshot[ 'target' ] == [ 'out' ]


def test_merge_concatenates_parameters() -> None:
    params_a = pl.DataFrame( [ { 'par_group': 'a', 'par_type': 'fixed', 'par_name': 'op_idn',
                                  'par_feature': None, 'node_idx': 0, 'parent_idx': None } ],
                              schema = ParameterParser().schema )
    params_b = pl.DataFrame( [ { 'par_group': 'b', 'par_type': 'fixed', 'par_name': 'op_idn',
                                  'par_feature': None, 'node_idx': 0, 'parent_idx': None } ],
                              schema = ParameterParser().schema )
    a = Cell( main_id='a', parameters=params_a )
    b = Cell( main_id='b', parameters=params_b )
    merged = Cell.merge( a, b )
    assert merged.tables[ '__table_ops_parameters__' ].shape[ 0 ] == 2


# -- register_node: declarative bookkeeping


def test_register_node_infers_kind() -> None:
    child = Cell( main_id='child', source=[ 'inp' ], target=[ 'out' ] )
    c = Cell( main_id='c', ops={ 'double': _double_op, 'kid': child } )
    c.register_node( 'double', source=[ 'inp' ], target=[ 'out' ] )
    c.register_node( 'kid',    source=[ 'inp' ], target=[ 'out2' ] )

    rows = { r[ 'node_idn' ]: r[ 'kind' ] for r in c.tables[ '__nodes__' ].to_dicts() }
    assert rows == { 'double': 'op', 'kid': 'cell' }


def test_register_node_requires_prior_tableops_registration() -> None:
    c = Cell( main_id='c' )
    with pytest.raises( KeyError ):
        c.register_node( 'nonexistent', source=[ 'inp' ], target=[ 'out' ] )


def test_register_node_allows_duplicate_target_names() -> None:
    """Not a DAG -- more than one node may legitimately claim the same
    target name across different rounds (e.g. a loop body vs its escape
    branch)."""
    c = Cell( main_id='c', ops={ 'double': _double_op, 'incr': _incr_op } )
    c.register_node( 'double', source=[ 'a' ], target=[ 'shared' ] )
    c.register_node( 'incr',   source=[ 'b' ], target=[ 'shared' ] )
    assert c.tables[ '__nodes__' ].shape[ 0 ] == 2


def test_validate_nodes_catches_dangling_reference() -> None:
    c = Cell( main_id='c', ops={ 'double': _double_op } )
    c.register_node( 'double', source=[ 'inp' ], target=[ 'out' ] )
    del c.tableOps[ 'double' ]
    with pytest.raises( KeyError ):
        c.validate_nodes()


# -- Fan-out / fan-in via the wiring engine


def test_fan_out_one_target_feeds_two_nodes() -> None:
    c = Cell(
        main_id='c',
        ops={ 'double': _double_op, 'incr': _incr_op },
        source=[ 'inp' ], target=[ 'doubled', 'incremented' ],
    )
    c.register_node( 'double', source=[ 'inp' ], target=[ 'doubled' ] )
    c.register_node( 'incr',   source=[ 'inp' ], target=[ 'incremented' ] )

    df_in = pl.DataFrame( { 'v': [ 1, 2, 3 ] } )
    doubled, incremented = c( ( df_in, ) ).result( timeout=5 )
    assert doubled[ 'v' ].to_list()     == [ 2, 4, 6 ]
    assert incremented[ 'v' ].to_list() == [ 2, 3, 4 ]


def _zip_op( dfs: tuple, verbose: int = 0, verbose_prefix: str = '' ) -> tuple:
    a, b = dfs
    return ( a.with_columns( ( pl.col( 'v' ) + b[ 'v' ] ).alias( 'v' ) ), )


def test_fan_in_waits_for_both_named_sources() -> None:
    c = Cell(
        main_id='c',
        ops={ 'double': _double_op, 'incr': _incr_op, 'zip': _zip_op },
        source=[ 'inp' ], target=[ 'out' ],
    )
    c.register_node( 'double', source=[ 'inp' ],             target=[ 'doubled' ] )
    c.register_node( 'incr',   source=[ 'inp' ],              target=[ 'incremented' ] )
    c.register_node( 'zip',    source=[ 'doubled', 'incremented' ], target=[ 'out' ] )

    df_in = pl.DataFrame( { 'v': [ 1, 2, 3 ] } )
    result = c( ( df_in, ) ).result( timeout=5 )
    assert result[ 0 ][ 'v' ].to_list() == [ 2 * 1 + ( 1 + 1 ), 2 * 2 + ( 2 + 1 ), 2 * 3 + ( 3 + 1 ) ]


# -- Loops: a stateless controller node fires null to the loop body and
# non-null to the escape branch (or vice versa) based purely on its input,
# no framework-level iteration cap. Because the Cell's own declared target
# resolves as soon as its name is merely *present* in values (regardless of
# null-ness), a controller must never write directly to the Cell's own
# target across multiple rounds -- route the escape branch through a
# passthrough node instead, which only fires once gen-tracking confirms a
# real (non-null) value arrived.


def _loop_controller_op( dfs: tuple, verbose: int = 0, verbose_prefix: str = '' ) -> tuple:
    n = dfs[ 0 ][ 'n' ][ 0 ]
    if n < 3:
        return ( pl.DataFrame( { 'n': [ n + 1 ] } ), None )
    return ( None, dfs[ 0 ] )


def _passthrough_op( dfs: tuple, verbose: int = 0, verbose_prefix: str = '' ) -> tuple:
    return ( dfs[ 0 ], )


def test_loop_terminates_via_null_vs_nonnull_targets() -> None:
    c = Cell(
        main_id='loop',
        ops={ 'controller': _loop_controller_op, 'passthrough': _passthrough_op },
        source=[ 'n' ], target=[ 'done' ],
    )
    c.register_node( 'controller',  source=[ 'n' ],      target=[ 'n', 'escape' ] )
    c.register_node( 'passthrough', source=[ 'escape' ], target=[ 'done' ] )

    result = c( ( pl.DataFrame( { 'n': [ 0 ] } ), ) ).result( timeout=5 )
    assert result[ 0 ][ 'n' ].to_list() == [ 3 ]


# -- Sequencing baked into a hand-written Actor (replaces TableProcessSequence)


def test_hand_written_sequencing_actor() -> None:
    """What used to be a declarative TableProcessSequence row is now plain
    Python inside a custom Actor's __call__, calling bind() on siblings."""

    class DoubleThenIncr:
        def __init__( self, cell: Cell ) -> None:
            self._cell = cell

        def __call__( self, dfs, verbose = 0, verbose_prefix = '' ) -> tuple:
            doubled = self._cell.bind( 'double' )( dfs, verbose=verbose, verbose_prefix=verbose_prefix ).result()
            return self._cell.bind( 'incr' )( doubled, verbose=verbose, verbose_prefix=verbose_prefix ).result()

    c = Cell( main_id='c', ops={ 'double': _double_op, 'incr': _incr_op }, source=[ 'inp' ], target=[ 'out' ] )
    c.tableOps[ 'sequence' ] = DoubleThenIncr( c )
    c.register_node( 'sequence', source=[ 'inp' ], target=[ 'out' ] )

    df_in = pl.DataFrame( { 'v': [ 1, 2, 3 ] } )
    result = c( ( df_in, ) ).result( timeout=5 )
    assert result[ 0 ][ 'v' ].to_list() == [ 3, 5, 7 ]


# -- BackendReadOp/BackendWriteOp wired through the __nodes__-shaped
# backend_read_process/backend_write_process factories.


def test_backend_write_then_read_via_nodes( tmp_path ) -> None:
    c = Cell(
        main_id = 'c',
        ops = {
            'read_it':  BackendReadOp(),
            'write_it': BackendWriteOp(),
        },
        source = [ 'to_write', 'backend_ref', '__backend_index__', '__parquet_backend__', '__sqlite_backend__', '__postgres_backend__' ],
        target = [ 'loaded' ],
    )
    write_row = backend_write_process(
        node_idn = 'write_it',
        source_data = 'to_write',
        source_backend_ref = 'backend_ref',
        target_status = 'written_ref',
        source_backend_index    = '__backend_index__',
        source_parquet_backend  = '__parquet_backend__',
        source_sqlite_backend   = '__sqlite_backend__',
        source_postgres_backend = '__postgres_backend__',
    )
    c.tables[ '__nodes__' ] = write_row
    read_row = backend_read_process(
        node_idn = 'read_it',
        source_backend_ref = 'written_ref',
        target_data = 'loaded',
        source_backend_index    = '__backend_index__',
        source_parquet_backend  = '__parquet_backend__',
        source_sqlite_backend   = '__sqlite_backend__',
        source_postgres_backend = '__postgres_backend__',
    )
    c.tables[ '__nodes__' ] = pl.concat( [ c.tables[ '__nodes__' ], read_row ], how = 'vertical' )

    fp = str( tmp_path / 'data.parquet' )
    to_write    = pl.DataFrame( { 'a': [ 1, 2 ] } )
    backend_ref = pl.DataFrame( { 'backend_id': [ 'b1' ] } )
    backend_index    = pl.DataFrame( { 'backend_id': [ 'b1' ], 'backend_table': [ '__parquet_backend__' ] } )
    parquet_backend  = pl.DataFrame( { 'backend_id': [ 'b1' ], 'fp': [ fp ], 'if_exists': [ 'append' ] } )
    sqlite_backend   = pl.DataFrame( schema = { 'backend_id': pl.Utf8, 'db_path': pl.Utf8, 'table_name': pl.Utf8, 'if_exists': pl.Utf8 } )
    postgres_backend = pl.DataFrame( schema = { 'backend_id': pl.Utf8, 'connection_string': pl.Utf8, 'table_name': pl.Utf8, 'schema_name': pl.Utf8, 'if_exists': pl.Utf8 } )

    result = c( (
        to_write, backend_ref, backend_index, parquet_backend, sqlite_backend, postgres_backend,
    ) ).result( timeout=5 )

    assert sorted( result[ 0 ][ 'a' ].to_list() ) == [ 1, 2 ]
