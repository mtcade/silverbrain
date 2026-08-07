#
#//  test_exprParse.py
#//  silverbrain
#//

"""
Tests for exprParse.py's text grammar -- see its module docstring for the
grammar and dtype-inference rules.
"""

import polars as pl
import pytest

from silverbrain.bdd import Atom, BDDManager, Cond, ColVar, Difference, Intersect, Union
from silverbrain.exprParse import expr_to_text, parse_expr
from silverbrain.schema import cannonical_schema


def test_parse_atom_with_explicit_dtype() -> None:
    assert parse_expr( "Atom('a', Int64)" ) == Atom( 'a', pl.Int64 )


def test_parse_atom_infers_via_omitted_arg() -> None:
    assert parse_expr( "Atom('op_idn')" ) == Atom( 'op_idn', cannonical_schema[ 'op_idn' ] )


def test_parse_atom_infers_via_sentinel() -> None:
    assert parse_expr( "Atom('op_idn', ?)" ) == Atom( 'op_idn', cannonical_schema[ 'op_idn' ] )


def test_parse_bare_string_is_atom_sugar_with_inference() -> None:
    assert parse_expr( "'op_idn'" ) == Atom( 'op_idn', cannonical_schema[ 'op_idn' ] )


def test_parse_raises_when_name_in_neither_map_nor_lookup() -> None:
    with pytest.raises( ValueError, match = 'unknown_col' ):
        parse_expr( "'unknown_col'" )


def test_parse_accepts_custom_dtype_lookup() -> None:
    expr = parse_expr( "'myname'", dtype_lookup = { 'myname': pl.Float64 } )
    assert expr == Atom( 'myname', pl.Float64 )


def test_parse_colvar() -> None:
    assert parse_expr( "ColVar('C')" ) == ColVar( 'C' )


def test_parse_nested_expression() -> None:
    m = BDDManager()
    expr = parse_expr( "Union(ColVar('C'), Atom('nu_col_0', Int64), 'op_idn')" )
    expected = Union( ColVar( 'C' ), Atom( 'nu_col_0', pl.Int64 ), Atom( 'op_idn', cannonical_schema[ 'op_idn' ] ) )
    assert expr.to_bdd( m ) is expected.to_bdd( m )


def test_parse_difference_intersect_cond() -> None:
    m = BDDManager()
    assert parse_expr( "Difference(Atom('a', Int64), Atom('b', Int64))" ).to_bdd( m ) \
        is Difference( Atom( 'a', pl.Int64 ), Atom( 'b', pl.Int64 ) ).to_bdd( m )
    assert parse_expr( "Intersect(Atom('a', Int64), Atom('b', Int64))" ).to_bdd( m ) \
        is Intersect( Atom( 'a', pl.Int64 ), Atom( 'b', pl.Int64 ) ).to_bdd( m )
    assert parse_expr( "Cond(ColVar('flag'), Atom('a', Int64), Atom('b', Int64))" ).to_bdd( m ) \
        is Cond( ColVar( 'flag' ), Atom( 'a', pl.Int64 ), Atom( 'b', pl.Int64 ) ).to_bdd( m )


def test_parse_empty_union_is_false() -> None:
    from silverbrain.bdd import FALSE
    m = BDDManager()
    assert parse_expr( 'Union()' ).to_bdd( m ) is FALSE


def test_parse_parametric_dtype_round_trips() -> None:
    expr = parse_expr( "Atom('x', List(Int64))" )
    assert expr == Atom( 'x', pl.List( pl.Int64 ) )


def test_expr_to_text_always_explicit() -> None:
    text = expr_to_text( Union( ColVar( 'C' ), Atom( 'a', pl.Int64 ) ) )
    assert text == "Union(ColVar('C'), Atom('a', Int64))"


def test_round_trip_through_text() -> None:
    m = BDDManager()
    expr = Cond(
        ColVar( 'flag' ),
        Union( Atom( 'a', pl.Int64 ), ColVar( 'C' ) ),
        Difference( Atom( 'b', pl.Utf8 ), Atom( 'c', pl.Float64 ) ),
    )
    rebuilt = parse_expr( expr_to_text( expr ) )
    assert rebuilt.to_bdd( m ) is expr.to_bdd( m )
