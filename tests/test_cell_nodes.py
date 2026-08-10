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

import threading

import polars as pl
import pytest

from silverbrain.cell import Cell
from silverbrain.parameterParse import ParameterParser


def _double_op( dfs: tuple, corr_id: tuple = (), verbose: int = 0, verbose_prefix: str = '' ) -> tuple:
    return ( dfs[ 0 ].with_columns( ( pl.col( 'v' ) * 2 ).alias( 'v' ) ), )


def _incr_op( dfs: tuple, corr_id: tuple = (), verbose: int = 0, verbose_prefix: str = '' ) -> tuple:
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


# -- "_" discard target: computed (side effects included) but never
# stored/wired, so nothing can depend on it.


def test_discard_target_is_not_stored_or_wired() -> None:
    calls: list = []

    def _write_op( dfs: tuple, corr_id: tuple = (), verbose: int = 0, verbose_prefix: str = '' ) -> tuple:
        calls.append( dfs[ 0 ][ 'v' ].to_list() )
        return ( dfs[ 0 ], )

    c = Cell(
        main_id='c',
        ops={ 'write': _write_op, 'consumer': _double_op },
        source=[ 'inp' ], target=[ 'out' ],
    )
    c.register_node( 'write',    source=[ 'inp' ], target=[ '_' ] )
    c.register_node( 'consumer', source=[ '_' ],   target=[ 'out' ] )

    df_in = pl.DataFrame( { 'v': [ 1, 2, 3 ] } )
    fut = c( ( df_in, ) )
    with pytest.raises( RuntimeError ):
        fut.result( timeout=5 )
    assert calls == [ [ 1, 2, 3 ] ]  # 'write' still ran; 'consumer' never fired


def test_discard_target_mixed_with_real_target() -> None:
    def _split_op( dfs: tuple, corr_id: tuple = (), verbose: int = 0, verbose_prefix: str = '' ) -> tuple:
        return ( dfs[ 0 ], dfs[ 0 ].with_columns( ( pl.col( 'v' ) * 10 ).alias( 'v' ) ) )

    c = Cell(
        main_id='c',
        ops={ 'split': _split_op },
        source=[ 'inp' ], target=[ 'kept' ],
    )
    c.register_node( 'split', source=[ 'inp' ], target=[ '_', 'kept' ] )

    df_in  = pl.DataFrame( { 'v': [ 1, 2, 3 ] } )
    result = c( ( df_in, ) ).result( timeout=5 )
    assert result[ 0 ][ 'v' ].to_list() == [ 10, 20, 30 ]


def test_multiple_nodes_may_all_discard_to_underscore() -> None:
    """"_" is exempt from any implicit uniqueness expectation -- multiple
    nodes may declare target=["_"] with no conflict, same as any other
    repeated target name (test_register_node_allows_duplicate_target_names)."""
    c = Cell( main_id='c', ops={ 'double': _double_op, 'incr': _incr_op } )
    c.register_node( 'double', source=[ 'a' ], target=[ '_' ] )
    c.register_node( 'incr',   source=[ 'b' ], target=[ '_' ] )
    assert c.tables[ '__nodes__' ].shape[ 0 ] == 2


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


def _zip_op( dfs: tuple, corr_id: tuple = (), verbose: int = 0, verbose_prefix: str = '' ) -> tuple:
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
# no framework-level iteration cap. A null token is an ordinary value now
# (not specially suppressed by the engine), so a controller reading its own
# prior output as a source (a self-referencing loop) must itself guard
# against being re-invoked with a null input once it has emitted its own
# termination signal -- the engine will happily keep dispatching it
# otherwise. The Cell's own declared target resolves on each name's first
# NON-null token specifically (see _WiringEngine._scan), so routing the
# escape branch through a passthrough node isn't required for correctness
# here, but is kept to exercise fan-in-style node-to-node wiring too.


def _loop_controller_op( dfs: tuple, corr_id: tuple = (), verbose: int = 0, verbose_prefix: str = '' ) -> tuple:
    if dfs[ 0 ] is None:
        # Already terminated (see module comment above) -- stay inert.
        return ( None, None )
    n = dfs[ 0 ][ 'n' ][ 0 ]
    if n < 3:
        return ( pl.DataFrame( { 'n': [ n + 1 ] } ), None )
    return ( None, dfs[ 0 ] )


def _passthrough_op( dfs: tuple, corr_id: tuple = (), verbose: int = 0, verbose_prefix: str = '' ) -> tuple:
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

        def __call__( self, dfs, corr_id = (), verbose = 0, verbose_prefix = '' ) -> tuple:
            doubled = self._cell.bind( 'double' )( dfs, verbose=verbose, verbose_prefix=verbose_prefix ).result()
            return self._cell.bind( 'incr' )( doubled, verbose=verbose, verbose_prefix=verbose_prefix ).result()

    c = Cell( main_id='c', ops={ 'double': _double_op, 'incr': _incr_op }, source=[ 'inp' ], target=[ 'out' ] )
    c.tableOps[ 'sequence' ] = DoubleThenIncr( c )
    c.register_node( 'sequence', source=[ 'inp' ], target=[ 'out' ] )

    df_in = pl.DataFrame( { 'v': [ 1, 2, 3 ] } )
    result = c( ( df_in, ) ).result( timeout=5 )
    assert result[ 0 ][ 'v' ].to_list() == [ 3, 5, 7 ]


# -- join='any'/'at_least_k', round_origin, fan-out cursor independence,
# and misaligned-round deadlock detection (Token/round-id wiring model)


def _record_op( calls: list ):
    """Returns an Actor that appends dfs[0] (or None) to `calls` and
    passes it through unchanged -- used to observe how many times/with
    what a node actually fired."""
    def _op( dfs: tuple, corr_id: tuple = (), verbose: int = 0, verbose_prefix: str = '' ) -> tuple:
        calls.append( dfs[ 0 ] if dfs[ 0 ] is None else dfs[ 0 ][ 'v' ].to_list() )
        return ( dfs[ 0 ], )
    return _op


def test_join_any_fires_with_only_one_of_two_sources_ever_written() -> None:
    calls: list = []
    c = Cell(
        main_id='c',
        ops={ 'merge_like': _record_op( calls ) },
        source=[ 'inp' ], target=[ 'out' ],
    )
    # 'never_written' is declared but nothing ever seeds/produces it.
    c.register_node( 'merge_like', source=[ 'inp', 'never_written' ], target=[ 'out' ], join='any' )

    df_in = pl.DataFrame( { 'v': [ 1 ] } )
    result = c( ( df_in, ) ).result( timeout=5 )
    assert result[ 0 ][ 'v' ].to_list() == [ 1 ]
    assert calls == [ [ 1 ] ]


def test_join_at_least_k_fires_once_k_sources_agree() -> None:
    calls: list = []
    c = Cell(
        main_id='c',
        ops={ 'quorum': _record_op( calls ) },
        source=[ 'a', 'b' ], target=[ 'out' ],
    )
    # 3 declared sources, only 2 ('a','b') ever get written -- 'c_unwritten'
    # never does; join_k=2 still lets it fire on 'a'+'b' alone.
    c.register_node( 'quorum', source=[ 'a', 'b', 'c_unwritten' ], target=[ 'out' ], join='at_least_k', join_k=2 )

    result = c( ( pl.DataFrame( { 'v': [ 1 ] } ), pl.DataFrame( { 'v': [ 2 ] } ) ) ).result( timeout=5 )
    assert result[ 0 ][ 'v' ].to_list() in ( [ 1 ], [ 2 ] )  # whichever 'a'/'b' resolved first, by declared order


def test_at_least_k_requires_join_k_in_valid_range() -> None:
    c = Cell( main_id='c', ops={ 'op': _passthrough_op }, source=[ 'a' ], target=[ 'out' ] )
    with pytest.raises( ValueError ):
        c.register_node( 'op', source=[ 'a' ], target=[ 'out' ], join='at_least_k', join_k=2 )  # k > len(source)
    with pytest.raises( ValueError ):
        c.register_node( 'op', source=[ 'a' ], target=[ 'out' ], join='at_least_k' )  # missing join_k
    with pytest.raises( ValueError ):
        c.register_node( 'op', source=[ 'a' ], target=[ 'out' ], join_k=1 )  # join_k without at_least_k


def test_corr_id_relays_unchanged_through_a_chain() -> None:
    """corr_id is purely a tracing aid now (see Token/_WiringEngine docs)
    -- every ordinary firing relays whatever corr_id its participating
    source port carried, unchanged, with no extension or re-minting."""
    seen: list = []

    def _relay( dfs: tuple, corr_id: tuple = (), verbose: int = 0, verbose_prefix: str = '' ) -> tuple:
        seen.append( corr_id )
        return ( dfs[ 0 ], )

    c = Cell(
        main_id='c',
        ops={ 'a': _relay, 'b': _relay },
        source=[ 'inp' ], target=[ 'out' ],
    )
    c.register_node( 'a', source=[ 'inp' ], target=[ 'mid' ] )
    c.register_node( 'b', source=[ 'mid' ], target=[ 'out' ] )

    root_corr_id = ( 'root-marker', )
    c( ( pl.DataFrame( { 'v': [ 1 ] } ), ), corr_id=root_corr_id ).result( timeout=5 )
    assert seen == [ root_corr_id, root_corr_id ]


def test_fan_out_cursors_are_independent_per_consumer() -> None:
    """'x' has a single token. Both 'fast' (source=['x']) and 'gate_setup'
    (source=['x']) read it independently and fire immediately; 'slow'
    (source=['x','gate']) also needs its own read of that SAME 'x' token,
    paired with 'gate_setup's output, once both are available. If cursors
    were shared/pop-based instead of per-(node_idn, name), 'fast' or
    'gate_setup' reading 'x' first would exhaust it, and 'slow' would
    deadlock waiting for an 'x' token that's already gone. 'fast' itself
    is dispatched concurrently with no ordering relative to the Cell's own
    resolution (its result is discarded, '_'), so it's observed via an
    Event rather than racily checked immediately after .result() returns."""
    fast_calls: list = []
    fast_done = threading.Event()

    def _fast( dfs: tuple, corr_id: tuple = (), verbose: int = 0, verbose_prefix: str = '' ) -> tuple:
        fast_calls.append( dfs[ 0 ][ 'v' ].to_list() )
        fast_done.set()
        return ( dfs[ 0 ], )

    c = Cell(
        main_id='c',
        ops={ 'fast': _fast, 'gate_setup': _passthrough_op, 'slow': _passthrough_op },
        source=[ 'x' ], target=[ 'slowed' ],
    )
    c.register_node( 'fast',       source=[ 'x' ],          target=[ '_' ] )
    c.register_node( 'gate_setup', source=[ 'x' ],          target=[ 'gate' ] )
    c.register_node( 'slow',       source=[ 'x', 'gate' ],  target=[ 'slowed' ] )

    result = c( ( pl.DataFrame( { 'v': [ 7 ] } ), ) ).result( timeout=5 )
    assert result[ 0 ][ 'v' ].to_list() == [ 7 ]
    assert fast_done.wait( timeout=5 )
    assert fast_calls == [ [ 7 ] ]


def test_rewire_is_an_unimplemented_future_stub() -> None:
    c = Cell( main_id='c', ops={ 'a': _passthrough_op }, source=[ 'inp' ], target=[ 'out' ] )
    c.register_node( 'a', source=[ 'inp' ], target=[ 'out' ] )
    with pytest.raises( NotImplementedError, match="FUTURE" ):
        c._rewire( 'a', [ 'somewhere_else' ] )


# BackendReadOp/BackendWriteOp wiring tests (backend_read_process/
# backend_write_process) live in silverflow/tests/test_cell_nodes_backend.py
# alongside the backend-specific TableOps they exercise.
