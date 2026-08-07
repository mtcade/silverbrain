#
#//  test_bdd.py
#//  silverbrain
#//

"""
Tests for bdd.py's canonical BDD algebra and set-expression AST. Canonical-
equality assertions always use `is` (never structural comparison of
BDDNodes -- see bdd.py's BDDNode docstring: equality is identity by
construction).
"""

import pytest

from silverbrain.bdd import (
    Atom,
    BDDManager,
    ColVar,
    Cond,
    Difference,
    FALSE,
    Intersect,
    TRUE,
    Union,
    bindings_to_polars,
    expr_to_polars,
    polars_to_bindings,
    polars_to_expr,
)


# -- BDD algebra: canonical identities


def test_atom_and_colvar_are_separate_namespaces() -> None:
    m = BDDManager()
    assert m.atom( 'C' ) is not m.colvar( 'C' )


def test_atom_is_stable_across_calls() -> None:
    m = BDDManager()
    assert m.atom( 'a' ) is m.atom( 'a' )


def test_intersect_is_idempotent() -> None:
    m = BDDManager()
    x = m.atom( 'x' )
    assert m.intersect( x, x ) is x


def test_union_is_idempotent() -> None:
    m = BDDManager()
    x = m.atom( 'x' )
    assert m.union( x, x ) is x


def test_difference_of_self_is_false() -> None:
    m = BDDManager()
    x = m.atom( 'x' )
    assert m.difference( x, x ) is FALSE


def test_union_is_commutative() -> None:
    m = BDDManager()
    a, b = m.atom( 'a' ), m.atom( 'b' )
    assert m.union( a, b ) is m.union( b, a )


def test_intersect_is_commutative() -> None:
    m = BDDManager()
    a, b = m.atom( 'a' ), m.atom( 'b' )
    assert m.intersect( a, b ) is m.intersect( b, a )


def test_negate_is_involutive() -> None:
    m = BDDManager()
    x = m.atom( 'x' )
    assert m.negate( m.negate( x ) ) is x


# -- compose: binding a variable and letting the result flow through


def test_compose_binds_colvar_into_expression() -> None:
    m = BDDManager()
    expr = Union( ColVar( 'C' ), Atom( 'nu_col_0' ) )
    c_idx = m._var_index( 'var', 'C' )

    bound = m.compose( expr.to_bdd( m ), c_idx, Atom( 'other' ).to_bdd( m ) )
    expected = Union( Atom( 'other' ), Atom( 'nu_col_0' ) ).to_bdd( m )
    assert bound is expected


def test_compose_with_false_removes_colvar_entirely() -> None:
    m = BDDManager()
    expr = Union( ColVar( 'C' ), Atom( 'a' ) )
    c_idx = m._var_index( 'var', 'C' )

    bound = m.compose( expr.to_bdd( m ), c_idx, FALSE )
    assert bound is Atom( 'a' ).to_bdd( m )


# -- resolve_to_columns: extracting a concrete set from a grounded BDD


def test_resolve_to_columns_worked_example() -> None:
    m = BDDManager()
    expr = Difference( Union( 'a', 'b' ), 'a' )
    assert m.resolve_to_columns( expr.to_bdd( m ) ) == frozenset( { 'b' } )


def test_resolve_to_columns_plain_union() -> None:
    m = BDDManager()
    expr = Union( 'a', 'b', 'c' )
    assert m.resolve_to_columns( expr.to_bdd( m ) ) == frozenset( { 'a', 'b', 'c' } )


def test_resolve_to_columns_intersect() -> None:
    m = BDDManager()
    expr = Intersect( Union( 'a', 'b' ), Union( 'b', 'c' ) )
    assert m.resolve_to_columns( expr.to_bdd( m ) ) == frozenset( { 'b' } )


def test_resolve_to_columns_raises_on_unbound_colvar() -> None:
    m = BDDManager()
    expr = Union( ColVar( 'C' ), Atom( 'a' ) )
    with pytest.raises( ValueError, match = 'C' ):
        m.resolve_to_columns( expr.to_bdd( m ) )


# -- forest serialization: round-trips through polars


def test_expr_round_trip_leaf() -> None:
    m = BDDManager()
    expr = Atom( 'nu_col_0' )
    df, next_idx = expr_to_polars( expr )
    assert next_idx == 1
    rebuilt = polars_to_expr( df )
    assert rebuilt.to_bdd( m ) is expr.to_bdd( m )


def test_expr_round_trip_nested() -> None:
    m = BDDManager()
    expr = Difference( Union( ColVar( 'C' ), 'nu_col_0' ), Intersect( 'a', 'b' ) )
    df, _ = expr_to_polars( expr )
    rebuilt = polars_to_expr( df )
    assert rebuilt.to_bdd( m ) is expr.to_bdd( m )


def test_expr_to_polars_difference_branch_column() -> None:
    df, _ = expr_to_polars( Difference( 'a', 'b' ) )
    left  = df.filter( df[ 'name' ] == 'a' )
    right = df.filter( df[ 'name' ] == 'b' )
    assert left[ 'branch' ][ 0 ] == 0
    assert right[ 'branch' ][ 0 ] == 1


def test_polars_to_expr_requires_root_hint_for_multi_root_forest() -> None:
    df, _ = bindings_to_polars( { 'C': Atom( 'a' ), 'D': Atom( 'b' ) } )
    with pytest.raises( ValueError ):
        polars_to_expr( df )


def test_bindings_round_trip() -> None:
    m = BDDManager()
    bindings = {
        'C': Union( 'nu_col_0', 'nu_col_1' ),
        'D': Difference( ColVar( 'C' ), 'nu_col_1' ),
    }
    df, next_idx = bindings_to_polars( bindings )
    assert next_idx == df.height

    rebuilt = polars_to_bindings( df )
    assert rebuilt.keys() == bindings.keys()
    for name in bindings:
        assert rebuilt[ name ].to_bdd( m ) is bindings[ name ].to_bdd( m )


def test_bindings_forest_shares_node_idx_counter() -> None:
    df, next_idx = bindings_to_polars( { 'C': Atom( 'a' ), 'D': Atom( 'b' ) }, node_idx_start = 10 )
    assert df[ 'node_idx' ].min() == 10
    assert next_idx == 12
    assert df[ 'node_idx' ].n_unique() == df.height


# -- predicates


def test_is_empty() -> None:
    m = BDDManager()
    assert m.is_empty( FALSE ) is True
    assert m.is_empty( m.atom( 'a' ) ) is False


def test_is_subset_and_is_superset() -> None:
    m = BDDManager()
    a, ab = Atom( 'a' ).to_bdd( m ), Union( 'a', 'b' ).to_bdd( m )
    assert m.is_subset( a, ab ) is True
    assert m.is_subset( ab, a ) is False
    assert m.is_superset( ab, a ) is True
    assert m.is_superset( a, ab ) is False


def test_intersect_of_distinct_atoms_is_empty_for_predicates() -> None:
    """Regression: two distinct literal atoms are disjoint, even though
    the raw boolean AND(x_a, x_b) is satisfiable (x_a=x_b=1) -- these
    predicates must not mistake that for a non-empty intersection."""
    m = BDDManager()
    ab_intersect = Intersect( 'a', 'b' ).to_bdd( m )
    assert m.is_empty( ab_intersect ) is False  # raw node, not the FALSE terminal...
    assert m.is_subset( ab_intersect, Atom( 'c' ).to_bdd( m ) ) is True  # ...but is disjointness-empty
    assert m.overlaps( ab_intersect, Atom( 'a' ).to_bdd( m ) ) is False
    assert m.overlaps( Atom( 'a' ).to_bdd( m ), Atom( 'b' ).to_bdd( m ) ) is False


def test_is_subset_holds_regardless_of_unbound_colvar_binding() -> None:
    m = BDDManager()
    expr = Union( ColVar( 'C' ), 'a' )
    assert m.contains( expr.to_bdd( m ), 'a' ) is True
    assert m.contains( ColVar( 'C' ).to_bdd( m ), 'a' ) is False


def test_overlaps_and_may_contain() -> None:
    m = BDDManager()
    assert m.may_contain( ColVar( 'C' ).to_bdd( m ), 'a' ) is True
    assert m.may_contain( Atom( 'b' ).to_bdd( m ), 'a' ) is False
    assert m.overlaps( Union( 'a', 'b' ).to_bdd( m ), Union( 'b', 'c' ).to_bdd( m ) ) is True
    assert m.overlaps( Atom( 'a' ).to_bdd( m ), Atom( 'b' ).to_bdd( m ) ) is False


# -- Cond: branching a generated schema on a flag


def test_cond_selects_then_when_flag_bound_true() -> None:
    m = BDDManager()
    expr = Cond( ColVar( 'flag' ), 'a', 'b' )
    flag_idx = m._var_index( 'var', 'flag' )
    resolved = m.compose( expr.to_bdd( m ), flag_idx, TRUE )
    assert resolved is Atom( 'a' ).to_bdd( m )


def test_cond_selects_else_when_flag_bound_false() -> None:
    m = BDDManager()
    expr = Cond( ColVar( 'flag' ), 'a', 'b' )
    flag_idx = m._var_index( 'var', 'flag' )
    resolved = m.compose( expr.to_bdd( m ), flag_idx, FALSE )
    assert resolved is Atom( 'b' ).to_bdd( m )


def test_negating_flag_equals_swapping_branches() -> None:
    """No Not/NotFlag exists -- negating a Cond's flag is always exactly
    equivalent to swapping its two branches (module docstring's
    'COND / FLAGS')."""
    m = BDDManager()
    flag = ColVar( 'flag' ).to_bdd( m )
    a, b = Atom( 'a' ).to_bdd( m ), Atom( 'b' ).to_bdd( m )

    negated_flag_cond = m.ite( m.negate( flag ), a, b )
    swapped_cond       = m.ite( flag, b, a )
    assert negated_flag_cond is swapped_cond


def test_cond_forest_round_trip() -> None:
    m = BDDManager()
    expr = Cond( ColVar( 'flag' ), Union( 'a', 'b' ), 'c' )
    df, _ = expr_to_polars( expr )

    root = df.filter( df[ 'parent_idx' ].is_null() )
    assert root[ 'kind' ][ 0 ] == 'cond'
    children = df.filter( df[ 'parent_idx' ] == root[ 'node_idx' ][ 0 ] ).sort( 'branch' )
    assert children[ 'branch' ].to_list() == [ 0, 1, 2 ]

    rebuilt = polars_to_expr( df )
    assert rebuilt.to_bdd( m ) is expr.to_bdd( m )
