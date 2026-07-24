#
#//  test_parameterParse.py
#//  silverbrain
#

import polars as pl
import pytest

from silverbrain.parameterParse import ParameterParser


def _make_parser() -> ParameterParser:
    p = ParameterParser()
    p.register( 'a',      pl.Int64 )
    p.register( 'b',      pl.Int64 )
    p.register( 'rho',    pl.Float64 )
    p.register( 'method', pl.Utf8 )  # exercises _COL_STR-typed rows (distribution/repeat/search-values)
    return p
#/def _make_parser


# -- par_idx (to_polars_flat, now derived by expanding to_polars()'s tree)

def test_flat_dict_par_idx_unchanged() -> None:
    p  = _make_parser()
    df, next_par_idx, _ = p.to_polars_flat( { 'X': { 'a': 1, 'b': 2 } } )
    assert sorted( df['par_idx'].unique().to_list() ) == [ 0 ]
    assert next_par_idx  == 1
#/def test_flat_dict_par_idx_unchanged


def test_or_par_idx_matches_pre_existing_numbering() -> None:
    # One sibling par_group crossed with a 2-alternative _or: par_idx should
    # be assigned exactly as before node_idx/parent_idx existed.
    p  = _make_parser()
    d  = { 'X': { 'a': 1 }, '_or': [ { 'X': { 'b': 2 } }, { 'X': { 'b': 3 } } ] }
    df, next_par_idx, _ = p.to_polars_flat( d )
    assert sorted( df['par_idx'].unique().to_list() ) == [ 0, 1 ]
    assert next_par_idx == 2
#/def test_or_par_idx_matches_pre_existing_numbering


# -- _or nested directly inside another _or: allowed now (explicit junction nodes)

def test_flat_or_directly_nested_in_or_does_not_raise() -> None:
    # Unlike the old to_polars_flat (removed), the tree-derived flat expansion
    # has no nesting restriction -- explicit junction nodes compose freely.
    p = _make_parser()
    d = {
        '_or': [
            { 'X': { 'a': 1 }, '_or': [ { 'X': { 'a': 2 } } ] },
        ],
    }
    p.to_polars_flat( d )  # should not raise
#/def test_flat_or_directly_nested_in_or_does_not_raise


def test_or_nested_inside_par_group_does_not_raise() -> None:
    p = _make_parser()
    d = {
        'X': {
            'a': 1,
            '_or': [
                { '_or': [ { 'rho': 0.1 } ] },
            ],
        },
    }
    p.to_polars_flat( d )  # should not raise
#/def test_or_nested_inside_par_group_does_not_raise


# -- _and (flat): new capability, no deprecated oracle to diff against

def test_flat_and_of_two_or_blocks_is_2x2_cross_product() -> None:
    # {'X': {'_and': [{'_or': [{'a':1},{'a':2}]}, {'_or': [{'b':1},{'b':2}]}]}}
    # -- two independent _or axes crossed via _and: 4 combinations, each with
    # exactly one 'a' value and one 'b' value.
    p = _make_parser()
    d = {
        'X': {
            '_and': [
                { '_or': [ { 'a': 1 }, { 'a': 2 } ] },
                { '_or': [ { 'b': 1 }, { 'b': 2 } ] },
            ],
        },
    }
    df, next_par_idx, _ = p.to_polars_flat( d )

    assert next_par_idx == 4
    assert sorted( df['par_idx'].unique().to_list() ) == [ 0, 1, 2, 3 ]
    assert df.shape[ 0 ] == 8  # 4 combos x 2 rows (one 'a', one 'b') each

    pairs = set()
    for par_idx, group in df.group_by( 'par_idx' ):
        a_val = group.filter( pl.col( 'par_name' ) == 'a' )['par_val_int64'][ 0 ]
        b_val = group.filter( pl.col( 'par_name' ) == 'b' )['par_val_int64'][ 0 ]
        pairs.add( ( a_val, b_val ) )
    assert pairs == { ( 1, 1 ), ( 1, 2 ), ( 2, 1 ), ( 2, 2 ) }
#/def test_flat_and_of_two_or_blocks_is_2x2_cross_product


def test_flat_top_level_or_bundles_multiple_par_groups_together() -> None:
    # Real-data shape: a fixed X plus a top-level _or bundling X's own 'b'
    # override + a second par_group Xk, per alternative -- each flat
    # combination must pair the RIGHT X.b with the RIGHT Xk.b, not cross
    # them independently.
    p = _make_parser()
    p.register( 'Xk', pl.Int64 )
    d = {
        'X': { 'a': 1 },
        '_or': [
            { 'X': { 'b': 10 }, 'Xk': { 'b': 20 } },
            { 'X': { 'b': 11 }, 'Xk': { 'b': 21 } },
        ],
    }
    df, next_par_idx, _ = p.to_polars_flat( d )

    assert next_par_idx == 2
    pairs = set()
    for par_idx, group in df.group_by( 'par_idx' ):
        x_b  = group.filter( ( pl.col( 'par_group' ) == 'X'  ) & ( pl.col( 'par_name' ) == 'b' ) )['par_val_int64'][ 0 ]
        xk_b = group.filter( ( pl.col( 'par_group' ) == 'Xk' ) & ( pl.col( 'par_name' ) == 'b' ) )['par_val_int64'][ 0 ]
        pairs.add( ( x_b, xk_b ) )
    # only the paired combinations from the same alternative -- never (10, 21) or (11, 20)
    assert pairs == { ( 10, 20 ), ( 11, 21 ) }
#/def test_flat_top_level_or_bundles_multiple_par_groups_together


# -- tree (to_polars/schema): _or, no duplication of shared content

def test_tree_or_no_duplication_of_base_content() -> None:
    # {'X': {'a': 1, '_or': [{'b': 2}, {'rho': 0.3}]}} -- 'a' must appear
    # exactly once in the tree, unlike the flat cross-product view.
    p = _make_parser()
    d = { 'X': { 'a': 1, '_or': [ { 'b': 2 }, { 'rho': 0.3 } ] } }
    df, next_node_idx = p.to_polars( d )

    a_rows = df.filter( pl.col( 'par_name' ) == 'a' )
    assert a_rows.shape[ 0 ] == 1

    base_node_idx = a_rows['node_idx'][ 0 ]
    or_junction   = df.filter( pl.col( 'par_type' ) == '_or' )
    assert or_junction.shape[ 0 ] == 1
    assert or_junction['parent_idx'][ 0 ] == base_node_idx

    or_node_idx = or_junction['node_idx'][ 0 ]
    alt_rows    = df.filter( pl.col( 'par_name' ).is_in( [ 'b', 'rho' ] ) )
    assert alt_rows.shape[ 0 ] == 2
    assert ( alt_rows['parent_idx'] == or_node_idx ).all()
    assert alt_rows['node_idx'].n_unique() == 2
    assert next_node_idx == df['node_idx'].max() + 1
#/def test_tree_or_no_duplication_of_base_content


def test_tree_schema_has_no_par_idx() -> None:
    p = _make_parser()
    schema = p.schema
    assert 'par_idx' not in schema
    assert 'node_idx' in schema
    assert 'parent_idx' in schema
#/def test_tree_schema_has_no_par_idx


# -- tree (to_polars/schema): _and, new capability

def test_tree_and_siblings_attach_under_one_and_junction() -> None:
    # {'X': {'_and': [{'a': 1}, {'b': 2}]}} -- both list items must attach as
    # AND-siblings under one '_and' junction node.
    p = _make_parser()
    d = { 'X': { '_and': [ { 'a': 1 }, { 'b': 2 } ] } }
    df, _ = p.to_polars( d )

    and_junction = df.filter( pl.col( 'par_type' ) == '_and' )
    assert and_junction.shape[ 0 ] == 1
    and_node_idx = and_junction['node_idx'][ 0 ]

    member_rows = df.filter( pl.col( 'par_name' ).is_in( [ 'a', 'b' ] ) )
    assert member_rows.shape[ 0 ] == 2
    assert ( member_rows['parent_idx'] == and_node_idx ).all()
    assert member_rows['node_idx'].n_unique() == 2
#/def test_tree_and_siblings_attach_under_one_and_junction


def test_tree_and_of_two_or_blocks_cross_product_shape() -> None:
    # {'X': {'_and': [{'_or': [{'a':1},{'a':2}]}, {'_or': [{'b':1},{'b':2}]}]}}
    # -- two independent _or axes crossed via _and; no natural owning key
    # needed for either axis.
    p = _make_parser()
    d = {
        'X': {
            '_and': [
                { '_or': [ { 'a': 1 }, { 'a': 2 } ] },
                { '_or': [ { 'b': 1 }, { 'b': 2 } ] },
            ],
        },
    }
    df, _ = p.to_polars( d )

    or_junctions = df.filter( pl.col( 'par_type' ) == '_or' )
    assert or_junctions.shape[ 0 ] == 2
    and_junctions = df.filter( pl.col( 'par_type' ) == '_and' )
    assert and_junctions.shape[ 0 ] == 1
    and_node_idx = and_junctions['node_idx'][ 0 ]
    assert ( or_junctions['parent_idx'] == and_node_idx ).all()

    a_rows = df.filter( pl.col( 'par_name' ) == 'a' )
    b_rows = df.filter( pl.col( 'par_name' ) == 'b' )
    assert a_rows.shape[ 0 ] == 2
    assert b_rows.shape[ 0 ] == 2
#/def test_tree_and_of_two_or_blocks_cross_product_shape


# -- tree: nested junctions compose with no guard (unlike to_polars_flat)

def test_tree_or_directly_nested_in_or_does_not_raise() -> None:
    p = _make_parser()
    d = { '_or': [ { 'X': { 'a': 1 }, '_or': [ { 'X': { 'a': 2 } } ] } ] }
    p.to_polars( d )  # should not raise -- explicit junction nodes compose freely
#/def test_tree_or_directly_nested_in_or_does_not_raise


def test_tree_and_directly_nested_in_or_does_not_raise() -> None:
    p = _make_parser()
    d = { '_or': [ { 'X': { '_and': [ { 'a': 1 }, { 'b': 2 } ] } } ] }
    p.to_polars( d )  # should not raise
#/def test_tree_and_directly_nested_in_or_does_not_raise


# -- tree: top-level _or bundling multiple par_groups per alternative

def test_tree_top_level_or_bundles_multiple_par_groups() -> None:
    p = _make_parser()
    d = {
        'X': { 'a': 1 },
        '_or': [
            { 'X': { 'b': 2 } },
            { 'X': { 'rho': 0.5 } },
        ],
    }
    df, _ = p.to_polars( d )

    x_base = df.filter( pl.col( 'par_name' ) == 'a' )
    assert x_base.shape[ 0 ] == 1  # base X content not duplicated

    or_junction = df.filter( pl.col( 'par_type' ) == '_or' )
    assert or_junction.shape[ 0 ] == 1
    assert or_junction['parent_idx'][ 0 ] is None  # top-level _or has no owning par_group
#/def test_tree_top_level_or_bundles_multiple_par_groups


def test_tree_disambiguates_fixed_sibling_from_or_alternatives() -> None:
    # The motivating ambiguous case: X's fixed content alongside Y's two _or
    # alternatives, all logically "children of the root". node_idx/parent_idx
    # alone couldn't tell these apart before par_type='_or' junctions existed.
    p = _make_parser()
    d = {
        'X': { 'a': 1 },
        '_or': [
            { 'Y': { 'b': 2 } },
            { 'Y': { 'rho': 0.5 } },
        ],
    }
    df, _ = p.to_polars( d )

    x_node = df.filter( pl.col( 'par_name' ) == 'a' )['node_idx'][ 0 ]
    assert df.filter( pl.col( 'node_idx' ) == x_node )['parent_idx'][ 0 ] is None

    or_node = df.filter( pl.col( 'par_type' ) == '_or' )['node_idx'][ 0 ]
    assert df.filter( pl.col( 'node_idx' ) == or_node )['parent_idx'][ 0 ] is None
    # X's node and the _or junction are siblings under the root, but only the
    # _or junction's own children (each alternative's '_and' wrapper) are
    # OR-related -- each wrapper bundles that alternative's own par_groups.
    wrappers = df.filter( ( pl.col( 'par_type' ) == '_and' ) & ( pl.col( 'parent_idx' ) == or_node ) )
    assert wrappers.shape[ 0 ] == 2
    y_rows = df.filter( pl.col( 'par_group' ) == 'Y' )
    assert y_rows.shape[ 0 ] == 2
    assert set( y_rows['parent_idx'].to_list() ) == set( wrappers['node_idx'].to_list() )
#/def test_tree_disambiguates_fixed_sibling_from_or_alternatives


def test_tree_matches_real_data_shape() -> None:
    # Structural analogue of the synth_0 bundle shape: a fixed X, an Xk with
    # its own _or, and a top-level _or bundling y+importances per alternative.
    p = _make_parser()
    d = {
        'X':  { 'a': 1 },
        'Xk': { '_or': [ { 'b': 2 }, { 'b': 3 } ] },
        '_or': [
            { 'X': { 'rho': 0.1 } },
            { 'X': { 'rho': 0.2 } },
        ],
    }
    df, _ = p.to_polars( d )

    assert df.filter( pl.col( 'par_name' ) == 'a' ).shape[ 0 ] == 1
    assert df.filter( pl.col( 'par_type' ) == '_or' ).shape[ 0 ] == 2  # Xk's + top-level
    assert df.filter( pl.col( 'par_name' ) == 'rho' ).shape[ 0 ] == 2
#/def test_tree_matches_real_data_shape


# -- repeat (constant/independent) on distribution-based random specs

def test_repeat_row_defaults_to_constant() -> None:
    p = _make_parser()
    d = { 'X': { 'rho': { 'distribution': 'uniform', 'min': 0.0, 'max': 1.0 } } }
    df, _ = p.to_polars( d )
    row = df.filter( ( pl.col( 'par_name' ) == 'rho' ) & ( pl.col( 'par_feature' ) == 'repeat' ) )
    assert row.shape[ 0 ] == 1
    assert row[ 'par_val_string' ][ 0 ] == 'constant'
#/def test_repeat_row_defaults_to_constant


def test_repeat_row_reflects_explicit_independent() -> None:
    p = _make_parser()
    d = { 'X': { 'rho': { 'distribution': 'uniform', 'min': 0.0, 'max': 1.0, 'repeat': 'independent' } } }
    df, _ = p.to_polars( d )
    row = df.filter( ( pl.col( 'par_name' ) == 'rho' ) & ( pl.col( 'par_feature' ) == 'repeat' ) )
    assert row.shape[ 0 ] == 1
    assert row[ 'par_val_string' ][ 0 ] == 'independent'
#/def test_repeat_row_reflects_explicit_independent


def test_repeat_invalid_value_raises() -> None:
    p = _make_parser()
    d = { 'X': { 'rho': { 'distribution': 'uniform', 'min': 0.0, 'max': 1.0, 'repeat': 'bogus' } } }
    with pytest.raises( ValueError ):
        p.to_polars( d )
#/def test_repeat_invalid_value_raises


def test_repeat_on_values_random_raises() -> None:
    p = _make_parser()
    d = { 'X': { 'rho': { 'values_random': [ 0.1, 0.2 ], 'repeat': 'independent' } } }
    with pytest.raises( ValueError ):
        p.to_polars( d )
#/def test_repeat_on_values_random_raises


def test_repeat_on_step_search_raises() -> None:
    p = _make_parser()
    d = { 'X': { 'rho': { 'start': 0.1, 'step': 0.1, 'repeat': 'independent' } } }
    with pytest.raises( ValueError ):
        p.to_polars( d )
#/def test_repeat_on_step_search_raises


# -- values_random / values_search (literal candidate lists)

def test_values_random_scalar_dtype_round_trips() -> None:
    p = _make_parser()
    d = { 'X': { 'method': { 'values_random': [ 'a', 'b', 'c' ] } } }
    df, _ = p.to_polars( d )
    row = df.filter( ( pl.col( 'par_name' ) == 'method' ) & ( pl.col( 'par_feature' ) == 'value' ) )
    assert row.shape[ 0 ] == 1
    assert row[ 'par_type' ][ 0 ] == 'random'
    assert list( row[ 'par_val_list_string' ][ 0 ] ) == [ 'a', 'b', 'c' ]
#/def test_values_random_scalar_dtype_round_trips


def test_values_search_scalar_dtype_round_trips() -> None:
    p = _make_parser()
    d = { 'X': { 'method': { 'values_search': [ 'a', 'b', 'c' ] } } }
    df, _ = p.to_polars( d )
    row = df.filter( ( pl.col( 'par_name' ) == 'method' ) & ( pl.col( 'par_feature' ) == 'value' ) )
    assert row.shape[ 0 ] == 1
    assert row[ 'par_type' ][ 0 ] == 'search'
    assert list( row[ 'par_val_list_string' ][ 0 ] ) == [ 'a', 'b', 'c' ]
#/def test_values_search_scalar_dtype_round_trips


def test_values_random_list_dtype_round_trips() -> None:
    p = _make_parser()
    p.register( 'modes_magnitude', pl.List( pl.Float64 ) )
    d = { 'X': { 'modes_magnitude': { 'values_random': [ [ 1.0, 2.0 ], [ 3.0, 4.0 ] ] } } }
    df, _ = p.to_polars( d )
    row = df.filter( ( pl.col( 'par_name' ) == 'modes_magnitude' ) & ( pl.col( 'par_feature' ) == 'value' ) )
    assert row.shape[ 0 ] == 1
    assert row[ 'par_val_list_list_float64' ].to_list()[ 0 ] == [ [ 1.0, 2.0 ], [ 3.0, 4.0 ] ]
#/def test_values_random_list_dtype_round_trips


def test_register_list_dtype_twice_adds_one_wrapped_column() -> None:
    p = ParameterParser()
    p.register( 'categories',         pl.List( pl.Int64 ) )
    p.register( 'categories_per_var', pl.List( pl.Int64 ) )
    assert list( p._list_wrapped_cols.keys() ).count( 'par_val_list_list_int64' ) == 1
    assert p.schema_flat[ 'par_val_list_list_int64' ] == pl.List( pl.List( pl.Int64 ) )
#/def test_register_list_dtype_twice_adds_one_wrapped_column


def test_values_random_casts_int_to_float() -> None:
    p = _make_parser()
    d = { 'X': { 'rho': { 'values_random': [ 1, 2, 3 ] } } }
    df, _ = p.to_polars( d )
    row = df.filter( ( pl.col( 'par_name' ) == 'rho' ) & ( pl.col( 'par_feature' ) == 'value' ) )
    assert list( row[ 'par_val_list_float64' ][ 0 ] ) == [ 1.0, 2.0, 3.0 ]
#/def test_values_random_casts_int_to_float


def test_values_random_rejects_wrong_type() -> None:
    p = _make_parser()
    d = { 'X': { 'a': { 'values_random': [ 'x', 'y' ] } } }
    with pytest.raises( ValueError ):
        p.to_polars( d )
#/def test_values_random_rejects_wrong_type


def test_values_random_rejects_lossy_float() -> None:
    p = _make_parser()
    d = { 'X': { 'a': { 'values_random': [ 1.5 ] } } }
    with pytest.raises( ValueError ):
        p.to_polars( d )
#/def test_values_random_rejects_lossy_float


def test_values_random_rejects_empty_list() -> None:
    p = _make_parser()
    d = { 'X': { 'rho': { 'values_random': [] } } }
    with pytest.raises( ValueError ):
        p.to_polars( d )
#/def test_values_random_rejects_empty_list


def test_values_random_unsupported_dtype_raises() -> None:
    p = _make_parser()
    p.register( 'standardize_y', pl.Boolean )
    d = { 'X': { 'standardize_y': { 'values_random': [ True, False ] } } }
    with pytest.raises( ValueError ):
        p.to_polars( d )
#/def test_values_random_unsupported_dtype_raises


# -- deprecated oracle: frozen pre-refactor behavior (regression safety net)
#
# Mirrors the tests above but targets to_polars_deprecated/schema_deprecated,
# which are never modified again after this. Later refactor stages diff their
# new implementations' output against this oracle.

def test_deprecated_flat_dict_par_idx_unchanged() -> None:
    p  = _make_parser()
    df, next_par_idx, next_node_idx = p.to_polars_deprecated( { 'X': { 'a': 1, 'b': 2 } } )
    assert sorted( df['par_idx'].unique().to_list() ) == [ 0 ]
    assert next_par_idx  == 1
    assert next_node_idx == 1
#/def test_deprecated_flat_dict_par_idx_unchanged


def test_deprecated_groups_par_idx_matches_pre_existing_numbering() -> None:
    p  = _make_parser()
    d  = { 'X': { 'a': 1 }, '_groups': [ { 'X': { 'b': 2 } }, { 'X': { 'b': 3 } } ] }
    df, next_par_idx, _ = p.to_polars_deprecated( d )
    assert sorted( df['par_idx'].unique().to_list() ) == [ 1, 2 ]
    assert next_par_idx == 3
#/def test_deprecated_groups_par_idx_matches_pre_existing_numbering


def test_deprecated_flat_dict_single_node_null_parent() -> None:
    p  = _make_parser()
    df, _, _ = p.to_polars_deprecated( { 'X': { 'a': 1, 'b': 2 } } )
    assert df['node_idx'].n_unique() == 1
    assert df['parent_idx'].is_null().all()
#/def test_deprecated_flat_dict_single_node_null_parent


def test_deprecated_groups_inside_par_group_chains_to_parent() -> None:
    p  = _make_parser()
    d  = { 'X': { 'a': 1, '_groups': [ { 'rho': 0.1 }, { 'rho': 0.2 } ] } }
    df, _, _ = p.to_polars_deprecated( d )

    base_node_idx = df.filter( pl.col( 'par_name' ) == 'a' )['node_idx'][ 0 ]
    rho_rows      = df.filter( pl.col( 'par_name' ) == 'rho' )
    assert rho_rows.shape[ 0 ] == 2
    assert ( rho_rows['parent_idx'] == base_node_idx ).all()
    assert rho_rows['node_idx'].n_unique() == 2
    assert base_node_idx not in rho_rows['node_idx'].to_list()
#/def test_deprecated_groups_inside_par_group_chains_to_parent


def test_deprecated_sibling_par_groups_do_not_parent_each_other() -> None:
    p  = _make_parser()
    d  = { 'X': { 'a': 1 }, 'Y': { 'b': 2 } }
    df, _, _ = p.to_polars_deprecated( d )

    x_node = df.filter( pl.col( 'par_group' ) == 'X' )['node_idx'][ 0 ]
    y_node = df.filter( pl.col( 'par_group' ) == 'Y' )['node_idx'][ 0 ]
    assert x_node != y_node
    assert ( df.filter( pl.col( 'par_group' ) == 'X' )['parent_idx'] ).is_null().all()
    assert ( df.filter( pl.col( 'par_group' ) == 'Y' )['parent_idx'] ).is_null().all()
#/def test_deprecated_sibling_par_groups_do_not_parent_each_other


def test_deprecated_groups_directly_nested_in_groups_raises() -> None:
    p = _make_parser()
    d = {
        '_groups': [
            { 'X': { 'a': 1 }, '_groups': [ { 'X': { 'a': 2 } } ] },
        ],
    }
    with pytest.raises( ValueError, match = '_groups cannot be nested directly' ):
        p.to_polars_deprecated( d )
#/def test_deprecated_groups_directly_nested_in_groups_raises


def test_deprecated_schema_includes_node_and_parent_idx() -> None:
    p = _make_parser()
    schema = p.schema_deprecated
    assert 'node_idx'   in schema
    assert 'parent_idx' in schema
    assert 'par_idx'    in schema
#/def test_deprecated_schema_includes_node_and_parent_idx
