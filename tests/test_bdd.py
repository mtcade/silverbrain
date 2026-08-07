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

import polars as pl
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
    apply_bindings,
    bindings_to_polars,
    close_bindings,
    colvar_names,
    decompile,
    expr_from_dict,
    expr_to_polars,
    polars_to_bindings,
    polars_to_expr,
    polars_to_signatures,
    resolve_signatures,
    signatures_to_polars,
)

# Atom now carries a dtype -- this module doesn't care which one for most
# tests, so a single shared dtype (and a terse local constructor) keeps the
# tests readable. bdd.py itself has no bare-string shorthand; A() is pure
# test-file sugar, not part of the public API.
_T = pl.Int64

def A( name: str ) -> Atom:
    return Atom( name, _T )
#/def A


# -- BDD algebra: canonical identities


def test_atom_and_colvar_are_separate_namespaces() -> None:
    m = BDDManager()
    assert m.atom( 'C', _T ) is not m.colvar( 'C' )


def test_atom_is_stable_across_calls() -> None:
    m = BDDManager()
    assert m.atom( 'a', _T ) is m.atom( 'a', _T )


def test_atom_raises_on_conflicting_dtype() -> None:
    m = BDDManager()
    m.atom( 'a', pl.Int64 )
    with pytest.raises( ValueError, match = 'a' ):
        m.atom( 'a', pl.Utf8 )


def test_intersect_is_idempotent() -> None:
    m = BDDManager()
    x = m.atom( 'x', _T )
    assert m.intersect( x, x ) is x


def test_union_is_idempotent() -> None:
    m = BDDManager()
    x = m.atom( 'x', _T )
    assert m.union( x, x ) is x


def test_difference_of_self_is_false() -> None:
    m = BDDManager()
    x = m.atom( 'x', _T )
    assert m.difference( x, x ) is FALSE


def test_union_is_commutative() -> None:
    m = BDDManager()
    a, b = m.atom( 'a', _T ), m.atom( 'b', _T )
    assert m.union( a, b ) is m.union( b, a )


def test_intersect_is_commutative() -> None:
    m = BDDManager()
    a, b = m.atom( 'a', _T ), m.atom( 'b', _T )
    assert m.intersect( a, b ) is m.intersect( b, a )


def test_negate_is_involutive() -> None:
    m = BDDManager()
    x = m.atom( 'x', _T )
    assert m.negate( m.negate( x ) ) is x


# -- compose: binding a variable and letting the result flow through


def test_compose_binds_colvar_into_expression() -> None:
    m = BDDManager()
    expr = Union( ColVar( 'C' ), A( 'nu_col_0' ) )
    c_idx = m._var_index( 'var', 'C' )

    bound = m.compose( expr.to_bdd( m ), c_idx, A( 'other' ).to_bdd( m ) )
    expected = Union( A( 'other' ), A( 'nu_col_0' ) ).to_bdd( m )
    assert bound is expected


def test_compose_with_false_removes_colvar_entirely() -> None:
    m = BDDManager()
    expr = Union( ColVar( 'C' ), A( 'a' ) )
    c_idx = m._var_index( 'var', 'C' )

    bound = m.compose( expr.to_bdd( m ), c_idx, FALSE )
    assert bound is A( 'a' ).to_bdd( m )


# -- resolve_to_columns / resolve_to_typed_columns: extracting a concrete set from a grounded BDD


def test_resolve_to_columns_worked_example() -> None:
    m = BDDManager()
    expr = Difference( Union( A( 'a' ), A( 'b' ) ), A( 'a' ) )
    assert m.resolve_to_columns( expr.to_bdd( m ) ) == frozenset( { 'b' } )


def test_resolve_to_columns_plain_union() -> None:
    m = BDDManager()
    expr = Union( A( 'a' ), A( 'b' ), A( 'c' ) )
    assert m.resolve_to_columns( expr.to_bdd( m ) ) == frozenset( { 'a', 'b', 'c' } )


def test_resolve_to_columns_intersect() -> None:
    m = BDDManager()
    expr = Intersect( Union( A( 'a' ), A( 'b' ) ), Union( A( 'b' ), A( 'c' ) ) )
    assert m.resolve_to_columns( expr.to_bdd( m ) ) == frozenset( { 'b' } )


def test_resolve_to_columns_raises_on_unbound_colvar() -> None:
    m = BDDManager()
    expr = Union( ColVar( 'C' ), A( 'a' ) )
    with pytest.raises( ValueError, match = 'C' ):
        m.resolve_to_columns( expr.to_bdd( m ) )


def test_resolve_to_typed_columns() -> None:
    m = BDDManager()
    expr = Union( Atom( 'a', pl.Int64 ), Atom( 'b', pl.Utf8 ) )
    assert m.resolve_to_typed_columns( expr.to_bdd( m ) ) == { 'a': pl.Int64, 'b': pl.Utf8 }


# -- forest serialization: round-trips through polars


def test_expr_round_trip_leaf() -> None:
    m = BDDManager()
    expr = A( 'nu_col_0' )
    df, next_idx = expr_to_polars( expr )
    assert next_idx == 1
    rebuilt = polars_to_expr( df )
    assert rebuilt.to_bdd( m ) is expr.to_bdd( m )


def test_expr_round_trip_nested() -> None:
    m = BDDManager()
    expr = Difference( Union( ColVar( 'C' ), A( 'nu_col_0' ) ), Intersect( A( 'a' ), A( 'b' ) ) )
    df, _ = expr_to_polars( expr )
    rebuilt = polars_to_expr( df )
    assert rebuilt.to_bdd( m ) is expr.to_bdd( m )


def test_expr_round_trip_preserves_dtype() -> None:
    m = BDDManager()
    expr = Atom( 'a', pl.Float64 )
    df, _ = expr_to_polars( expr )
    rebuilt = polars_to_expr( df )
    assert rebuilt == Atom( 'a', pl.Float64 )


def test_expr_to_polars_difference_branch_column() -> None:
    df, _ = expr_to_polars( Difference( A( 'a' ), A( 'b' ) ) )
    left  = df.filter( df[ 'name' ] == 'a' )
    right = df.filter( df[ 'name' ] == 'b' )
    assert left[ 'branch' ][ 0 ] == 0
    assert right[ 'branch' ][ 0 ] == 1


def test_polars_to_expr_requires_root_hint_for_multi_root_forest() -> None:
    df, _ = bindings_to_polars( { 'C': A( 'a' ), 'D': A( 'b' ) } )
    with pytest.raises( ValueError ):
        polars_to_expr( df )


def test_bindings_round_trip() -> None:
    m = BDDManager()
    bindings = {
        'C': Union( A( 'nu_col_0' ), A( 'nu_col_1' ) ),
        'D': Difference( ColVar( 'C' ), A( 'nu_col_1' ) ),
    }
    df, next_idx = bindings_to_polars( bindings )
    assert next_idx == df.height

    rebuilt = polars_to_bindings( df )
    assert rebuilt.keys() == bindings.keys()
    for name in bindings:
        assert rebuilt[ name ].to_bdd( m ) is bindings[ name ].to_bdd( m )


def test_bindings_forest_shares_node_idx_counter() -> None:
    df, next_idx = bindings_to_polars( { 'C': A( 'a' ), 'D': A( 'b' ) }, node_idx_start = 10 )
    assert df[ 'node_idx' ].min() == 10
    assert next_idx == 12
    assert df[ 'node_idx' ].n_unique() == df.height


# -- predicates


def test_is_empty() -> None:
    m = BDDManager()
    assert m.is_empty( FALSE ) is True
    assert m.is_empty( m.atom( 'a', _T ) ) is False


def test_is_subset_and_is_superset() -> None:
    m = BDDManager()
    a, ab = A( 'a' ).to_bdd( m ), Union( A( 'a' ), A( 'b' ) ).to_bdd( m )
    assert m.is_subset( a, ab ) is True
    assert m.is_subset( ab, a ) is False
    assert m.is_superset( ab, a ) is True
    assert m.is_superset( a, ab ) is False


def test_intersect_of_distinct_atoms_is_empty_for_predicates() -> None:
    """Regression: two distinct literal atoms are disjoint, even though
    the raw boolean AND(x_a, x_b) is satisfiable (x_a=x_b=1) -- these
    predicates must not mistake that for a non-empty intersection."""
    m = BDDManager()
    ab_intersect = Intersect( A( 'a' ), A( 'b' ) ).to_bdd( m )
    assert m.is_empty( ab_intersect ) is False  # raw node, not the FALSE terminal...
    assert m.is_subset( ab_intersect, A( 'c' ).to_bdd( m ) ) is True  # ...but is disjointness-empty
    assert m.overlaps( ab_intersect, A( 'a' ).to_bdd( m ) ) is False
    assert m.overlaps( A( 'a' ).to_bdd( m ), A( 'b' ).to_bdd( m ) ) is False


def test_is_subset_holds_regardless_of_unbound_colvar_binding() -> None:
    m = BDDManager()
    expr = Union( ColVar( 'C' ), A( 'a' ) )
    assert m.contains( expr.to_bdd( m ), 'a' ) is True
    assert m.contains( ColVar( 'C' ).to_bdd( m ), 'a' ) is False


def test_overlaps_and_may_contain() -> None:
    m = BDDManager()
    assert m.may_contain( ColVar( 'C' ).to_bdd( m ), 'a' ) is True
    assert m.may_contain( A( 'b' ).to_bdd( m ), 'a' ) is False
    assert m.overlaps( Union( A( 'a' ), A( 'b' ) ).to_bdd( m ), Union( A( 'b' ), A( 'c' ) ).to_bdd( m ) ) is True
    assert m.overlaps( A( 'a' ).to_bdd( m ), A( 'b' ).to_bdd( m ) ) is False


# -- Cond: branching a generated schema on a flag


def test_cond_selects_then_when_flag_bound_true() -> None:
    m = BDDManager()
    expr = Cond( ColVar( 'flag' ), A( 'a' ), A( 'b' ) )
    flag_idx = m._var_index( 'var', 'flag' )
    resolved = m.compose( expr.to_bdd( m ), flag_idx, TRUE )
    assert resolved is A( 'a' ).to_bdd( m )


def test_cond_selects_else_when_flag_bound_false() -> None:
    m = BDDManager()
    expr = Cond( ColVar( 'flag' ), A( 'a' ), A( 'b' ) )
    flag_idx = m._var_index( 'var', 'flag' )
    resolved = m.compose( expr.to_bdd( m ), flag_idx, FALSE )
    assert resolved is A( 'b' ).to_bdd( m )


def test_negating_flag_equals_swapping_branches() -> None:
    """No Not/NotFlag exists -- negating a Cond's flag is always exactly
    equivalent to swapping its two branches (module docstring's
    'COND / FLAGS')."""
    m = BDDManager()
    flag = ColVar( 'flag' ).to_bdd( m )
    a, b = A( 'a' ).to_bdd( m ), A( 'b' ).to_bdd( m )

    negated_flag_cond = m.ite( m.negate( flag ), a, b )
    swapped_cond       = m.ite( flag, b, a )
    assert negated_flag_cond is swapped_cond


def test_cond_forest_round_trip() -> None:
    m = BDDManager()
    expr = Cond( ColVar( 'flag' ), Union( A( 'a' ), A( 'b' ) ), A( 'c' ) )
    df, _ = expr_to_polars( expr )

    root = df.filter( df[ 'parent_idx' ].is_null() )
    assert root[ 'kind' ][ 0 ] == 'cond'
    children = df.filter( df[ 'parent_idx' ] == root[ 'node_idx' ][ 0 ] ).sort( 'branch' )
    assert children[ 'branch' ].to_list() == [ 0, 1, 2 ]

    rebuilt = polars_to_expr( df )
    assert rebuilt.to_bdd( m ) is expr.to_bdd( m )


# -- decompile: reconstructing an Expr tree from a compiled BDDNode


def test_decompile_false_round_trips() -> None:
    m = BDDManager()
    assert decompile( m, FALSE ).to_bdd( m ) is FALSE


def test_decompile_raises_on_true_terminal() -> None:
    """decompile() must raise, not guess, if ever handed TRUE directly --
    it would need the excluded universal-set/complement notion to express
    it (see bdd.py's 'RESOLUTION' docstring section)."""
    m = BDDManager()
    with pytest.raises( ValueError, match = 'universal-set' ):
        decompile( m, TRUE )


def test_decompile_round_trips_atom() -> None:
    m = BDDManager()
    expr = A( 'a' )
    assert decompile( m, expr.to_bdd( m ) ).to_bdd( m ) is expr.to_bdd( m )


def test_decompile_round_trips_nested_expression() -> None:
    m = BDDManager()
    expr = Union( A( 'a' ), Intersect( A( 'b' ), ColVar( 'C' ) ) )
    node = expr.to_bdd( m )
    assert decompile( m, node ).to_bdd( m ) is node


def test_decompile_raises_when_result_needs_the_universal_set() -> None:
    """Regression: TRUE can legitimately appear on a LOW cofactor, not
    just a HIGH one -- resolving Difference(Union(ColVar('C'), 'a', 'b'),
    Intersect('a', ColVar('D'))) hits exactly this: at a=0 the subtracted
    term is false regardless of ColVar('D'), so the difference is
    unconditionally true there, regardless of D -- which has no
    Union/Intersect/Difference/Atom/ColVar representation."""
    m = BDDManager()
    expr = Difference( Union( ColVar( 'C' ), A( 'a' ), A( 'b' ) ), Intersect( A( 'a' ), ColVar( 'D' ) ) )
    node = expr.to_bdd( m )
    with pytest.raises( ValueError, match = 'universal-set' ):
        decompile( m, node )


# -- close_bindings / apply_bindings / resolve_signatures


def test_close_bindings_resolves_a_dependency_chain() -> None:
    """closed['D']'s raw BDD isn't required to simplify down to plain
    Atom('b') -- compose() is pure boolean substitution, it doesn't know
    'a'/'b' are disjoint the way resolve_to_columns does (see bdd.py's
    predicates NOTE ON CORRECTNESS) -- so the semantic check goes through
    resolve_to_columns, not raw `is` identity against a hand-simplified
    node."""
    m = BDDManager()
    bindings = {
        'C': Union( A( 'a' ), A( 'b' ) ),
        'D': Difference( ColVar( 'C' ), A( 'a' ) ),
    }
    closed = close_bindings( m, bindings )
    assert m.resolve_to_columns( closed[ 'D' ] ) == frozenset( { 'b' } )


def test_close_bindings_leaves_external_colvar_untouched() -> None:
    m = BDDManager()
    bindings = { 'C': Union( ColVar( 'External' ), A( 'a' ) ) }
    closed = close_bindings( m, bindings )
    assert closed[ 'C' ] is Union( ColVar( 'External' ), A( 'a' ) ).to_bdd( m )


def test_close_bindings_raises_on_cycle() -> None:
    m = BDDManager()
    bindings = {
        'C': Union( ColVar( 'D' ), A( 'x' ) ),
        'D': Union( ColVar( 'C' ), A( 'y' ) ),
    }
    with pytest.raises( ValueError, match = 'Cyclic' ):
        close_bindings( m, bindings )


def test_apply_bindings_composes_every_closed_binding() -> None:
    m = BDDManager()
    closed = { 'C': A( 'a' ).to_bdd( m ) }
    node = Union( ColVar( 'C' ), A( 'b' ) ).to_bdd( m )
    assert apply_bindings( m, node, closed ) is Union( A( 'a' ), A( 'b' ) ).to_bdd( m )


def test_resolve_signatures_fully_resolves_when_covered() -> None:
    m = BDDManager()
    signatures = { 'sig': Union( ColVar( 'C' ), A( 'extra' ) ) }
    bindings = { 'C': Union( A( 'a' ), A( 'b' ) ) }
    resolved = resolve_signatures( m, signatures, bindings )
    assert resolved[ 'sig' ].to_bdd( m ) is Union( A( 'a' ), A( 'b' ), A( 'extra' ) ).to_bdd( m )


def test_resolve_signatures_leaves_unbound_colvar_in_place() -> None:
    m = BDDManager()
    signatures = { 'sig': Union( ColVar( 'Unbound' ), A( 'extra' ) ) }
    resolved = resolve_signatures( m, signatures, {} )
    assert resolved[ 'sig' ].to_bdd( m ) is Union( ColVar( 'Unbound' ), A( 'extra' ) ).to_bdd( m )


def test_resolve_signatures_resolves_through_a_binding_chain() -> None:
    """Same caveat as test_close_bindings_resolves_a_dependency_chain --
    check via resolve_to_columns, not raw BDD identity."""
    m = BDDManager()
    signatures = { 'sig': ColVar( 'D' ) }
    bindings = {
        'C': Union( A( 'a' ), A( 'b' ) ),
        'D': Difference( ColVar( 'C' ), A( 'a' ) ),
    }
    resolved = resolve_signatures( m, signatures, bindings )
    assert m.resolve_to_columns( resolved[ 'sig' ].to_bdd( m ) ) == frozenset( { 'b' } )


# -- signature forest serialization


def test_signatures_round_trip() -> None:
    m = BDDManager()
    signatures = {
        ( 'op_a', 'output', 0 ): Union( A( 'a' ), A( 'b' ) ),
        ( 'op_b', 'input', 0 ):  Difference( ColVar( 'C' ), A( 'a' ) ),
    }
    df, next_idx = signatures_to_polars( signatures )
    assert next_idx == df.height

    rebuilt = polars_to_signatures( df )
    assert rebuilt.keys() == signatures.keys()
    for key in signatures:
        assert rebuilt[ key ].to_bdd( m ) is signatures[ key ].to_bdd( m )


def test_polars_to_signatures_raises_on_duplicate_root_key() -> None:
    df, _ = signatures_to_polars( { ( 'op_a', 'output', 0 ): A( 'a' ) } )
    dupe_row = df.filter( df[ 'node_idx' ] == 0 ).with_columns( pl.lit( 1, dtype = pl.UInt32 ).alias( 'node_idx' ) )
    df = pl.concat( [ df, dupe_row ], how = 'vertical' )
    with pytest.raises( ValueError, match = 'Duplicate' ):
        polars_to_signatures( df )


def test_polars_to_bindings_raises_on_duplicate_var_name() -> None:
    df, _ = bindings_to_polars( { 'C': A( 'a' ) } )
    dupe_row = df.filter( df[ 'node_idx' ] == 0 ).with_columns( pl.lit( 1, dtype = pl.UInt32 ).alias( 'node_idx' ) )
    df = pl.concat( [ df, dupe_row ], how = 'vertical' )
    with pytest.raises( ValueError, match = 'Duplicate' ):
        polars_to_bindings( df )


# -- colvar_names: pure AST walk, no BDDManager needed


def test_colvar_names_atom_and_colvar_leaves() -> None:
    assert colvar_names( A( 'a' ) ) == set()
    assert colvar_names( ColVar( 'C' ) ) == { 'C' }


def test_colvar_names_nested() -> None:
    expr = Cond(
        ColVar( 'flag' ),
        Union( A( 'a' ), ColVar( 'C' ) ),
        Difference( ColVar( 'D' ), A( 'b' ) ),
    )
    assert colvar_names( expr ) == { 'flag', 'C', 'D' }


def test_colvar_names_empty_union_has_none() -> None:
    assert colvar_names( Union() ) == set()


# -- Expr.to_dict()/expr_from_dict(): plain dict serialization


def test_atom_dict_round_trip() -> None:
    expr = Atom( 'a', pl.Int64 )
    assert expr_from_dict( expr.to_dict() ) == expr


def test_colvar_dict_round_trip() -> None:
    expr = ColVar( 'C' )
    assert expr_from_dict( expr.to_dict() ) == expr


def test_nested_expr_dict_round_trip() -> None:
    m = BDDManager()
    expr = Cond(
        ColVar( 'flag' ),
        Union( A( 'a' ), ColVar( 'C' ) ),
        Difference( A( 'b' ), A( 'c' ) ),
    )
    rebuilt = expr_from_dict( expr.to_dict() )
    assert rebuilt.to_bdd( m ) is expr.to_bdd( m )
