#
#//  test_actor.py
#//  silverbrain
#//

"""
Tests for the Actor protocol's declared input/output interface (§1.6 of
nested-gathering-russell.md): InputSchema/OutputSchema as a property of
Actor, replacing Web's declare()/NodeDecl exposure machinery.
"""

import warnings

import polars as pl

from silverbrain.cell import Cell
from silverbrain.tableOps import InputSchema, OutputSchema


def test_cell_input_output_default_to_none() -> None:
    c = Cell( main_id='c' )
    assert c.input  is None
    assert c.output is None


def test_cell_input_output_stored_as_declared() -> None:
    in_schema  = InputSchema( columns={ 'x': pl.Int64 } )
    out_schema = OutputSchema( columns={ 'y': pl.Int64 } )
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
        input   = [ InputSchema( columns={ 'v': pl.Int64 } ) ],
        output  = [ OutputSchema( columns={ 'v': pl.Int64 } ) ],
    )

    with warnings.catch_warnings():
        warnings.simplefilter( 'ignore', DeprecationWarning )
        parent = Cell( main_id='parent', ops={ 'kid': child } )

    rows = parent.tables[ '__table_op_schema__' ].filter( pl.col( 'op_idn' ) == 'kid' ).to_dicts()
    directions = { r[ 'direction' ] for r in rows }
    assert directions == { 'input', 'output' }
    assert next( r for r in rows if r[ 'direction' ] == 'input' )[ 'column_names' ] == [ 'v' ]
    assert next( r for r in rows if r[ 'direction' ] == 'output' )[ 'column_names' ] == [ 'v' ]


def test_registering_cell_without_schema_does_not_crash() -> None:
    """A nested Cell with no declared input/output (the common case) must not
    trip register_tableOps's effects lookup -- Cell has no `effects` attribute."""
    child = Cell( main_id='child' )
    with warnings.catch_warnings():
        warnings.simplefilter( 'ignore', DeprecationWarning )
        parent = Cell( main_id='parent', ops={ 'kid': child } )
    assert parent.tables[ '__table_op_schema__' ].filter( pl.col( 'op_idn' ) == 'kid' ).is_empty()
