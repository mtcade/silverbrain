#
#//  test_tableOps_resolve.py
#//  silverbrain
#//

"""
ResolveExpressionOp resolves a signature forest (bdd.signatures_to_polars
schema) against a bindings forest (bdd.bindings_to_polars schema),
substituting as many ColVars as possible and leaving the rest -- see
bdd.py's "RESOLUTION" docstring section for the algorithm.
"""

import polars as pl

from silverbrain.bdd import (
    Atom,
    BDDManager,
    ColVar,
    Union,
    bindings_to_polars,
    polars_to_signatures,
    signatures_to_polars,
)
from silverbrain.tableOps import ResolveExpressionOp

_T = pl.Int64

def A( name: str ) -> Atom:
    return Atom( name, _T )
#/def A


def test_resolve_expression_op_fully_resolves_when_binding_covers_everything() -> None:
    signatures_df, _ = signatures_to_polars( {
        ( 'my_op', 'output', 0 ): Union( ColVar( 'C' ), A( 'extra' ) ),
    } )
    bindings_df, _ = bindings_to_polars( { 'C': Union( A( 'a' ), A( 'b' ) ) } )

    op = ResolveExpressionOp()
    ( result_df, ) = op( ( signatures_df, bindings_df ) )

    resolved = polars_to_signatures( result_df )
    m = BDDManager()
    assert resolved[ ( 'my_op', 'output', 0 ) ].to_bdd( m ) is Union( A( 'a' ), A( 'b' ), A( 'extra' ) ).to_bdd( m )


def test_resolve_expression_op_leaves_unbound_colvar_in_place() -> None:
    signatures_df, _ = signatures_to_polars( {
        ( 'my_op', 'input', 0 ): Union( ColVar( 'Unbound' ), A( 'extra' ) ),
    } )
    bindings_df, _ = bindings_to_polars( {} )

    op = ResolveExpressionOp()
    ( result_df, ) = op( ( signatures_df, bindings_df ) )

    resolved = polars_to_signatures( result_df )
    m = BDDManager()
    assert resolved[ ( 'my_op', 'input', 0 ) ].to_bdd( m ) is Union( ColVar( 'Unbound' ), A( 'extra' ) ).to_bdd( m )


def test_resolve_expression_op_shares_colvar_scope_across_actors() -> None:
    """ColVar scope is global -- the same C referenced by two different actors' signatures is the same variable."""
    signatures_df, _ = signatures_to_polars( {
        ( 'op_a', 'output', 0 ): ColVar( 'C' ),
        ( 'op_b', 'input', 0 ):  ColVar( 'C' ),
    } )
    bindings_df, _ = bindings_to_polars( { 'C': A( 'shared_col' ) } )

    op = ResolveExpressionOp()
    ( result_df, ) = op( ( signatures_df, bindings_df ) )

    resolved = polars_to_signatures( result_df )
    m = BDDManager()
    expected = A( 'shared_col' ).to_bdd( m )
    assert resolved[ ( 'op_a', 'output', 0 ) ].to_bdd( m ) is expected
    assert resolved[ ( 'op_b', 'input', 0 ) ].to_bdd( m ) is expected


def test_resolve_expression_op_declares_forbidden_extra_input_schema() -> None:
    """No ColVar in either declared input == 'forbidden' (no extra columns
    allowed) under the new expression-native InputSchema -- see
    schema.py's InputSchema docstring."""
    from silverbrain.bdd import colvar_names

    op = ResolveExpressionOp()
    assert colvar_names( op.input[ 0 ].expr ) == set()
    assert colvar_names( op.input[ 1 ].expr ) == set()
