#
#//  test_tableOps_placeholder.py
#//  silverbrain
#//

"""
Tests for PlaceholderOp's expr/columns constructor duality -- see its
docstring in tableOps.py.
"""

import polars as pl
import pytest

from silverbrain.bdd import Atom, BDDManager, ColVar, Union
from silverbrain.tableOps import PlaceholderOp


def test_columns_builds_a_union_of_atoms() -> None:
    op = PlaceholderOp( columns = { 'a': pl.Int64, 'b': pl.Utf8 } )
    m = BDDManager()
    assert m.resolve_to_typed_columns( op.expr.to_bdd( m ) ) == { 'a': pl.Int64, 'b': pl.Utf8 }


def test_expr_accepts_a_colvar_containing_expression() -> None:
    expr = Union( ColVar( 'C' ), Atom( 'a', pl.Int64 ) )
    op = PlaceholderOp( expr = expr )
    m = BDDManager()
    assert op.expr.to_bdd( m ) is expr.to_bdd( m )


def test_requires_exactly_one_of_expr_or_columns() -> None:
    with pytest.raises( ValueError ):
        PlaceholderOp()
    with pytest.raises( ValueError ):
        PlaceholderOp( expr = Atom( 'a', pl.Int64 ), columns = { 'a': pl.Int64 } )


def test_input_tolerates_extra_columns_output_does_not() -> None:
    op = PlaceholderOp( columns = { 'a': pl.Int64 } )
    m = BDDManager()
    assert m.may_contain( op.input[ 0 ].expr.to_bdd( m ), 'anything_else' ) is True
    assert m.may_contain( op.output[ 0 ].expr.to_bdd( m ), 'anything_else' ) is False


def test_call_always_raises() -> None:
    op = PlaceholderOp( columns = { 'a': pl.Int64 } )
    with pytest.raises( NotImplementedError ):
        op( ( pl.DataFrame( { 'a': [ 1 ] } ), ) )
