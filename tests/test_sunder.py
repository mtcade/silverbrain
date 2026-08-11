#
#//  test_sunder.py
#//  silverbrain
#//

"""
Tests for silverbrain.sunder: named single-row runtime-context tables
(e.g. _trigger) whose schema lives in schema.table_schemas like any other
table, the sunder_table_factories registry, and _WiringEngine's auto-supply
of them to any node that declares one as a source without this Cell's own
external contract (self.source) ever being able to provide it.
"""

import polars as pl

from silverbrain.cell import Cell
from silverbrain.schema import table_schemas
from silverbrain.sunder import sunder_table_factories


def _passthrough_op( dfs: tuple, corr_id: tuple = (), verbose: int = 0, verbose_prefix: str = '' ) -> tuple:
    return ( dfs[ 0 ], )


def _record_op( calls: list ):
    def _op( dfs: tuple, corr_id: tuple = (), verbose: int = 0, verbose_prefix: str = '' ) -> tuple:
        calls.append( dfs[ 0 ] )
        return ( dfs[ 0 ], )
    return _op


# -- sunder_table_factories / schema


def test_trigger_schema_lives_in_silverbrain_schema() -> None:
    assert table_schemas[ '_trigger' ] == {
        'run_id':       pl.Utf8,
        'triggered_at': pl.Datetime( time_zone = 'UTC' ),
    }


def test_trigger_factory_builds_one_row_matching_its_schema() -> None:
    df = sunder_table_factories[ '_trigger' ]()
    assert df.shape == ( 1, 2 )
    assert df.schema == table_schemas[ '_trigger' ]


def test_trigger_factory_run_id_is_fresh_each_call() -> None:
    first  = sunder_table_factories[ '_trigger' ]()
    second = sunder_table_factories[ '_trigger' ]()
    assert first[ 'run_id' ].item() != second[ 'run_id' ].item()


def test_factories_registry_is_extensible_by_merging() -> None:
    """The pattern an implementor (e.g. duckpond) follows to add its own
    sunder tables -- confirms nothing about this dict prevents it."""
    extra_factory    = lambda: pl.DataFrame( { 'foo': [ 1 ] } )
    merged_factories = sunder_table_factories | { '_extra': extra_factory }
    assert merged_factories[ '_trigger' ] is sunder_table_factories[ '_trigger' ]
    assert merged_factories[ '_extra' ]().to_dicts() == [ { 'foo': 1 } ]


# -- Cell-level auto-supply (_WiringEngine.__init__)


def test_cell_auto_supplies_sunder_table_to_a_node_not_in_cell_source() -> None:
    """'_trigger' is declared by a node but never appears in this Cell's
    own external source -- no caller could ever seed it, so the Cell
    builds and seeds it itself before the node ever fires."""
    calls: list = []
    c = Cell(
        main_id = 'c',
        ops     = { 'read': _record_op( calls ) },
        source  = [],
        target  = [ 'out' ],
    )
    c.register_node( 'read', source = [ '_trigger' ], target = [ 'out' ] )

    result = c( () ).result( timeout = 5 )
    assert result[ 0 ].schema == table_schemas[ '_trigger' ]
    assert calls[ 0 ].schema == table_schemas[ '_trigger' ]


def test_explicit_cell_source_value_wins_over_auto_supply() -> None:
    """When '_trigger' IS part of this Cell's own declared source, the
    caller's value is used -- auto-supply only ever kicks in for names a
    caller could never seed."""
    calls: list = []
    c = Cell(
        main_id = 'c',
        ops     = { 'read': _record_op( calls ) },
        source  = [ '_trigger' ],
        target  = [ 'out' ],
    )
    c.register_node( 'read', source = [ '_trigger' ], target = [ 'out' ] )

    supplied = pl.DataFrame(
        { 'run_id': [ 'caller-supplied' ], 'triggered_at': sunder_table_factories[ '_trigger' ]()[ 'triggered_at' ] },
        schema = table_schemas[ '_trigger' ],
    )
    result = c( ( supplied, ) ).result( timeout = 5 )
    assert result[ 0 ][ 'run_id' ].item() == 'caller-supplied'


def test_auto_supplied_sunder_table_is_shared_by_fan_out() -> None:
    """Two nodes both declaring '_trigger' as a source (neither in
    self.source) read the SAME auto-built token -- one factory call per
    Cell invocation, not one per consumer."""
    calls_a: list = []
    calls_b: list = []
    c = Cell(
        main_id = 'c',
        ops     = { 'a': _record_op( calls_a ), 'b': _record_op( calls_b ) },
        source  = [],
        target  = [ 'out_a', 'out_b' ],
    )
    c.register_node( 'a', source = [ '_trigger' ], target = [ 'out_a' ] )
    c.register_node( 'b', source = [ '_trigger' ], target = [ 'out_b' ] )

    c( () ).result( timeout = 5 )
    assert calls_a[ 0 ][ 'run_id' ].item() == calls_b[ 0 ][ 'run_id' ].item()


def test_explicit_node_target_wins_over_auto_supply() -> None:
    """A sibling node explicitly targets '_trigger' (a real, in-cell
    pass-through) -- even though '_trigger' is absent from this Cell's own
    self.source, auto-supply must NOT fire for it, since something inside
    this Cell can (and does) explicitly produce it. If auto-supply fired
    anyway, its synthetic token would land in the log before the real
    producer's, and the consumer would read the fake one first."""
    calls: list = []
    marker = pl.DataFrame( { 'marker': [ 'explicit-value' ] } )

    def _produce( dfs: tuple, corr_id: tuple = (), verbose: int = 0, verbose_prefix: str = '' ) -> tuple:
        return ( marker, )

    c = Cell(
        main_id = 'c',
        ops     = { 'produce': _produce, 'consume': _record_op( calls ) },
        source  = [ 'inp' ],
        target  = [ 'out' ],
    )
    c.register_node( 'produce', source = [ 'inp' ],      target = [ '_trigger' ] )
    c.register_node( 'consume', source = [ '_trigger' ], target = [ 'out' ] )

    df_in = pl.DataFrame( { 'v': [ 1 ] } )
    result = c( ( df_in, ) ).result( timeout = 5 )
    assert result[ 0 ].equals( marker )
    assert calls[ 0 ].equals( marker )


def test_nested_cell_auto_supplies_sunder_table_without_parent_relay() -> None:
    """A deeply-nested Cell's own internal node needs '_trigger'. Neither
    the inner nor the outer Cell declares '_trigger' anywhere in their own
    source/target contracts -- no relay chain is set up at all -- yet the
    inner Cell still resolves it, because auto-supply is a per-Cell
    concern, not something a parent must thread through."""
    calls: list = []
    # inner.source=['inp'] only exists to match arity with outer's node
    # registration below (Cell.__call__ matches by arity, never by name) --
    # 'inp' itself is never read by any node inside inner; only '_trigger'
    # (declared by 'read', absent from inner.source) is.
    inner = Cell(
        main_id = 'inner',
        ops     = { 'read': _record_op( calls ) },
        source  = [ 'inp' ],
        target  = [ 'out' ],
    )
    inner.register_node( 'read', source = [ '_trigger' ], target = [ 'out' ] )

    outer = Cell(
        main_id = 'outer',
        ops     = { 'kid': inner },
        source  = [ 'inp' ],
        target  = [ 'out' ],
    )
    outer.register_node( 'kid', source = [ 'inp' ], target = [ 'out' ] )

    df_in = pl.DataFrame( { 'v': [ 1 ] } )
    outer( ( df_in, ) ).result( timeout = 5 )
    assert calls[ 0 ].schema == table_schemas[ '_trigger' ]
