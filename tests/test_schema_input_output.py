#
#//  test_schema_input_output.py
#//  silverbrain
#//

"""
Tests for schema.py's expression-native InputSchema/OutputSchema --
extra/passthrough_from no longer exist as separate fields; the old
'forbidden'/'open'/'transparent' behaviors and passthrough linkage are now
consequences of ColVar presence/sharing in expr alone. See schema.py's
InputSchema/OutputSchema docstrings and passthrough_inputs().
"""

import polars as pl

from silverbrain.bdd import ColVar, colvar_names
from silverbrain.schema import InputSchema, OutputSchema, TableOpSchema, passthrough_inputs


def test_from_columns_forbidden_has_no_colvar() -> None:
    schema = InputSchema.from_columns( { 'a': pl.Int64 } )
    assert colvar_names( schema.expr ) == set()


def test_from_columns_open_has_an_unreferenced_colvar() -> None:
    schema = InputSchema.from_columns( { 'a': pl.Int64 }, extra = 'open' )
    names = colvar_names( schema.expr )
    assert len( names ) == 1


def test_from_columns_open_generates_distinct_colvars_per_call() -> None:
    """Each from_columns(..., extra='open') call must mint its own fresh
    ColVar -- ColVar scope is global (bdd.py's module docstring), so two
    unrelated ops' anonymous 'ignore extras' placeholders must never
    accidentally become the same shared variable."""
    a = InputSchema.from_columns( { 'a': pl.Int64 }, extra = 'open' )
    b = InputSchema.from_columns( { 'a': pl.Int64 }, extra = 'open' )
    assert colvar_names( a.expr ) != colvar_names( b.expr )


def test_from_columns_transparent_shares_given_colvar() -> None:
    rest = ColVar( 'rest' )
    inp  = InputSchema.from_columns( { 'a': pl.Int64 }, extra = rest )
    out  = OutputSchema.from_columns( { 'b': pl.Int64 }, passthrough = rest )
    assert colvar_names( inp.expr ) == { 'rest' }
    assert colvar_names( out.expr ) == { 'rest' }


def test_passthrough_inputs_finds_shared_colvar() -> None:
    rest = ColVar( 'rest' )
    schema = TableOpSchema(
        inputs  = [ InputSchema.from_columns( { 'a': pl.Int64 }, extra = rest ) ],
        outputs = [ OutputSchema.from_columns( { 'b': pl.Int64 }, passthrough = rest ) ],
    )
    assert passthrough_inputs( schema, output_index = 0 ) == [ 0 ]


def test_passthrough_inputs_empty_when_no_shared_colvar() -> None:
    schema = TableOpSchema(
        inputs  = [ InputSchema.from_columns( { 'a': pl.Int64 }, extra = 'open' ) ],
        outputs = [ OutputSchema.from_columns( { 'b': pl.Int64 } ) ],
    )
    assert passthrough_inputs( schema, output_index = 0 ) == []


def test_input_schema_dict_round_trip() -> None:
    schema = InputSchema.from_columns( { 'a': pl.Int64, 'b': pl.Utf8 }, extra = 'open' )
    rebuilt = InputSchema.from_dict( schema.to_dict() )
    assert rebuilt.expr == schema.expr


def test_table_op_schema_to_polars_schema_is_bdd_signature_forest() -> None:
    schema = TableOpSchema(
        inputs  = [ InputSchema.from_columns( { 'a': pl.Int64 } ) ],
        outputs = [ OutputSchema.from_columns( { 'b': pl.Int64 } ) ],
    )
    df, next_idx = schema.to_polars_schema( 'my_op' )
    assert next_idx == df.height
    assert set( df.filter( pl.col( 'parent_idx' ).is_null() )[ 'direction' ].to_list() ) == { 'input', 'output' }


def test_table_op_schema_to_polars_threads_node_idx_start() -> None:
    schema = TableOpSchema(
        inputs  = [ InputSchema.from_columns( { 'a': pl.Int64 } ) ],
        outputs = [ OutputSchema.from_columns( { 'b': pl.Int64 } ) ],
    )
    df, next_idx = schema.to_polars_schema( 'my_op', node_idx_start = 10 )
    assert df[ 'node_idx' ].min() == 10
    assert next_idx > 10
