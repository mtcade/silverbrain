#
#//  test_actor.py
#//  silverbrain
#//

"""
Tests for the Actor protocol's declared input/output interface:
InputSchema/OutputSchema as a property of Actor -- a genuine schema,
not routing metadata.
"""

import warnings

import polars as pl

from silverbrain.bdd import BDDManager, polars_to_signatures
from silverbrain.cell import Cell
from silverbrain.tableOps import InputSchema, OutputSchema


def test_cell_input_output_default_to_none() -> None:
    c = Cell( main_id='c' )
    assert c.input  is None
    assert c.output is None


def test_cell_input_output_stored_as_declared() -> None:
    in_schema  = InputSchema.from_columns( { 'x': pl.Int64 } )
    out_schema = OutputSchema.from_columns( { 'y': pl.Int64 } )
    c = Cell( main_id='c', input=[ in_schema ], output=[ out_schema ] )
    assert c.input  == [ in_schema ]
    assert c.output == [ out_schema ]


def test_nested_cell_schema_flows_into_parent_table_op_schema() -> None:
    """A nested Cell's declared input/output lands in the parent's __table_op_schema__,
    the same mechanism a raw TableOp's declared schema already uses."""
    child = Cell(
        main_id = 'child',
        source  = [ 'v' ],
        target  = [ 'v' ],
        input   = [ InputSchema.from_columns( { 'v': pl.Int64 } ) ],
        output  = [ OutputSchema.from_columns( { 'v': pl.Int64 } ) ],
    )

    with warnings.catch_warnings():
        warnings.simplefilter( 'ignore', DeprecationWarning )
        parent = Cell( main_id='parent', ops={ 'kid': child } )

    signatures = polars_to_signatures( parent.tables[ '__table_op_schema__' ] )
    directions = { key[1] for key in signatures if key[0] == 'kid' }
    assert directions == { 'input', 'output' }

    m = BDDManager()
    assert m.resolve_to_columns( signatures[ ( 'kid', 'input', 0 ) ].to_bdd( m ) ) == frozenset( { 'v' } )
    assert m.resolve_to_columns( signatures[ ( 'kid', 'output', 0 ) ].to_bdd( m ) ) == frozenset( { 'v' } )


def test_multiple_ops_get_globally_unique_node_idx_in_table_op_schema() -> None:
    """__table_op_schema__ is now a bdd forest, not a flat column-list --
    node_idx must stay unique across every op registered on a Cell, not
    just within one op's own signature (regression for the counter
    threaded through Cell._next_schema_node_idx / register_tableOps)."""
    kid_a = Cell( main_id='kid_a', source=[ 'v' ], target=[ 'v' ],
        input=[ InputSchema.from_columns( { 'v': pl.Int64 } ) ],
        output=[ OutputSchema.from_columns( { 'v': pl.Int64 } ) ] )
    kid_b = Cell( main_id='kid_b', source=[ 'v' ], target=[ 'v' ],
        input=[ InputSchema.from_columns( { 'v': pl.Int64 } ) ],
        output=[ OutputSchema.from_columns( { 'v': pl.Int64 } ) ] )

    with warnings.catch_warnings():
        warnings.simplefilter( 'ignore', DeprecationWarning )
        parent = Cell( main_id='parent', ops={ 'kid_a': kid_a, 'kid_b': kid_b } )

    df = parent.tables[ '__table_op_schema__' ]
    assert df[ 'node_idx' ].n_unique() == df.height

    signatures = polars_to_signatures( df )
    m = BDDManager()
    for key in ( ( 'kid_a', 'input', 0 ), ( 'kid_b', 'input', 0 ) ):
        assert m.resolve_to_columns( signatures[ key ].to_bdd( m ) ) == frozenset( { 'v' } )


def test_registering_cell_without_schema_does_not_crash() -> None:
    """A nested Cell with no declared input/output (the common case) must not
    trip register_tableOps's effects lookup -- Cell has no `effects` attribute."""
    child = Cell( main_id='child' )
    with warnings.catch_warnings():
        warnings.simplefilter( 'ignore', DeprecationWarning )
        parent = Cell( main_id='parent', ops={ 'kid': child } )
    assert parent.tables[ '__table_op_schema__' ].filter( pl.col( 'op_idn' ) == 'kid' ).is_empty()
