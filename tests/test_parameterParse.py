#
#//  test_parameterParse.py
#//  silverbrain
#

import polars as pl
import pytest

from silverbrain.parameterParse import (
    ParameterParser,
    op_ctor_kwargs_forest,
    parameter_group_to_tree,
)
from silverbrain.schema import table_schemas


def _make_parser() -> ParameterParser:
    p = ParameterParser()
    p.register( 'a',      pl.Int64 )
    p.register( 'b',      pl.Int64 )
    p.register( 'rho',    pl.Float64 )
    p.register( 'method', pl.Utf8 )  # exercises _COL_STR-typed rows (distribution/repeat/search-values)
    return p
#/def _make_parser


# -- _comment (reserved key, silently ignored everywhere own keys are walked)

def test_comment_key_ignored_in_tree_top_level() -> None:
    p = _make_parser()
    df, _ = p.to_polars( { '_comment': 'top-level note', 'a': 1 } )
    assert df[ 'par_name' ].drop_nulls().to_list() == [ 'a' ]
#/def test_comment_key_ignored_in_tree_top_level


def test_comment_key_ignored_inside_nested_group() -> None:
    p = _make_parser()
    df, _ = p.to_polars( { '_or': [ { 'a': 1, '_comment': 'inline note' } ] } )
    assert df[ 'par_name' ].drop_nulls().to_list() == [ 'a' ]
#/def test_comment_key_ignored_inside_nested_group


def test_comment_key_ignored_in_flat() -> None:
    p = _make_parser()
    df, next_par_idx, _ = p.to_polars_flat( { 'a': 1, '_comment': 'inline note' } )
    assert next_par_idx == 1
    assert df[ 'par_name' ].drop_nulls().to_list() == [ 'a' ]
#/def test_comment_key_ignored_in_flat


def test_comment_key_ignored_by_dtype_inference() -> None:
    tree, _ = parameter_group_to_tree(
        'reader', { 'threshold': 0.5, '_comment': 'why threshold=0.5' },
    )
    assert op_ctor_kwargs_forest( tree ) == { 'reader': { 'threshold': 0.5 } }
#/def test_comment_key_ignored_by_dtype_inference


# -- par_idx (to_polars_flat, now derived by expanding to_polars()'s tree)

def test_flat_dict_par_idx_unchanged() -> None:
    p  = _make_parser()
    df, next_par_idx, _ = p.to_polars_flat( { 'a': 1, 'b': 2 } )
    assert sorted( df['par_idx'].unique().to_list() ) == [ 0 ]
    assert next_par_idx  == 1
#/def test_flat_dict_par_idx_unchanged


def test_or_par_idx_matches_pre_existing_numbering() -> None:
    # A fixed sibling crossed with a 2-alternative _or: par_idx should be
    # assigned exactly as before node_idx/parent_idx existed.
    p  = _make_parser()
    d  = { 'a': 1, '_or': [ { 'b': 2 }, { 'b': 3 } ] }
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
            { 'a': 1, '_or': [ { 'a': 2 } ] },
        ],
    }
    p.to_polars_flat( d )  # should not raise
#/def test_flat_or_directly_nested_in_or_does_not_raise


def test_or_nested_inside_content_does_not_raise() -> None:
    p = _make_parser()
    d = {
        'a': 1,
        '_or': [
            { '_or': [ { 'rho': 0.1 } ] },
        ],
    }
    p.to_polars_flat( d )  # should not raise
#/def test_or_nested_inside_content_does_not_raise


# -- _and (flat)

def test_flat_and_of_two_or_blocks_is_2x2_cross_product() -> None:
    # {'_and': [{'_or': [{'a':1},{'a':2}]}, {'_or': [{'b':1},{'b':2}]}]}
    # -- two independent _or axes crossed via _and: 4 combinations, each with
    # exactly one 'a' value and one 'b' value.
    p = _make_parser()
    d = {
        '_and': [
            { '_or': [ { 'a': 1 }, { 'a': 2 } ] },
            { '_or': [ { 'b': 1 }, { 'b': 2 } ] },
        ],
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


def test_flat_or_alternative_bundles_multiple_par_names_together() -> None:
    # A fixed 'a' plus an _or whose alternatives each carry TWO correlated
    # par_names ('x_b'/'xk_b') as ordinary siblings in one dict -- no
    # wrapper key needed, since siblings in the same dict already share one
    # node_idx. Each flat combination must pair the RIGHT x_b with the
    # RIGHT xk_b, not cross them independently.
    p = _make_parser()
    p.register( 'x_b',  pl.Int64 )
    p.register( 'xk_b', pl.Int64 )
    d = {
        'a': 1,
        '_or': [
            { 'x_b': 10, 'xk_b': 20 },
            { 'x_b': 11, 'xk_b': 21 },
        ],
    }
    df, next_par_idx, _ = p.to_polars_flat( d )

    assert next_par_idx == 2
    pairs = set()
    for par_idx, group in df.group_by( 'par_idx' ):
        x_b  = group.filter( pl.col( 'par_name' ) == 'x_b'  )['par_val_int64'][ 0 ]
        xk_b = group.filter( pl.col( 'par_name' ) == 'xk_b' )['par_val_int64'][ 0 ]
        pairs.add( ( x_b, xk_b ) )
    # only the paired combinations from the same alternative -- never (10, 21) or (11, 20)
    assert pairs == { ( 10, 20 ), ( 11, 21 ) }
#/def test_flat_or_alternative_bundles_multiple_par_names_together


# -- tree (to_polars/schema): _or, no duplication of shared content

def test_tree_or_no_duplication_of_base_content() -> None:
    # {'a': 1, '_or': [{'b': 2}, {'rho': 0.3}]} -- 'a' must appear exactly
    # once in the tree, unlike the flat cross-product view.
    p = _make_parser()
    d = { 'a': 1, '_or': [ { 'b': 2 }, { 'rho': 0.3 } ] }
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
    # {'_and': [{'a': 1}, {'b': 2}]} -- both list items must attach as
    # AND-siblings under one '_and' junction node.
    p = _make_parser()
    d = { '_and': [ { 'a': 1 }, { 'b': 2 } ] }
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
    # {'_and': [{'_or': [{'a':1},{'a':2}]}, {'_or': [{'b':1},{'b':2}]}]}
    # -- two independent _or axes crossed via _and; no owning key needed for
    # either axis.
    p = _make_parser()
    d = {
        '_and': [
            { '_or': [ { 'a': 1 }, { 'a': 2 } ] },
            { '_or': [ { 'b': 1 }, { 'b': 2 } ] },
        ],
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
    d = { '_or': [ { 'a': 1, '_or': [ { 'a': 2 } ] } ] }
    p.to_polars( d )  # should not raise -- explicit junction nodes compose freely
#/def test_tree_or_directly_nested_in_or_does_not_raise


def test_tree_and_directly_nested_in_or_does_not_raise() -> None:
    p = _make_parser()
    d = { '_or': [ { '_and': [ { 'a': 1 }, { 'b': 2 } ] } ] }
    p.to_polars( d )  # should not raise
#/def test_tree_and_directly_nested_in_or_does_not_raise


# -- reserved keys: _or and _and are never allowed as siblings in one dict

def test_or_and_and_as_siblings_raises() -> None:
    # There is no case that genuinely needs both at once -- nest one inside
    # the other instead (see module docstring's RESERVED KEYS section).
    p = _make_parser()
    d = { '_or': [ { 'a': 1 } ], '_and': [ { 'b': 2 } ] }
    with pytest.raises( ValueError, match = "both '_or' and '_and'" ):
        p.to_polars( d )
#/def test_or_and_and_as_siblings_raises


def test_or_and_and_as_siblings_raises_when_nested() -> None:
    p = _make_parser()
    d = { '_or': [ { '_or': [ { 'a': 1 } ], '_and': [ { 'b': 2 } ] } ] }
    with pytest.raises( ValueError, match = "both '_or' and '_and'" ):
        p.to_polars( d )
#/def test_or_and_and_as_siblings_raises_when_nested


# -- repeat (constant/independent) on distribution-based random specs

def test_repeat_row_defaults_to_constant() -> None:
    p = _make_parser()
    d = { 'rho': { 'distribution': 'uniform', 'min': 0.0, 'max': 1.0 } }
    df, _ = p.to_polars( d )
    row = df.filter( ( pl.col( 'par_name' ) == 'rho' ) & ( pl.col( 'par_feature' ) == 'repeat' ) )
    assert row.shape[ 0 ] == 1
    assert row[ 'par_val_string' ][ 0 ] == 'constant'
#/def test_repeat_row_defaults_to_constant


def test_repeat_row_reflects_explicit_independent() -> None:
    p = _make_parser()
    d = { 'rho': { 'distribution': 'uniform', 'min': 0.0, 'max': 1.0, 'repeat': 'independent' } }
    df, _ = p.to_polars( d )
    row = df.filter( ( pl.col( 'par_name' ) == 'rho' ) & ( pl.col( 'par_feature' ) == 'repeat' ) )
    assert row.shape[ 0 ] == 1
    assert row[ 'par_val_string' ][ 0 ] == 'independent'
#/def test_repeat_row_reflects_explicit_independent


def test_repeat_invalid_value_raises() -> None:
    p = _make_parser()
    d = { 'rho': { 'distribution': 'uniform', 'min': 0.0, 'max': 1.0, 'repeat': 'bogus' } }
    with pytest.raises( ValueError ):
        p.to_polars( d )
#/def test_repeat_invalid_value_raises


def test_repeat_on_values_random_raises() -> None:
    p = _make_parser()
    d = { 'rho': { 'values_random': [ 0.1, 0.2 ], 'repeat': 'independent' } }
    with pytest.raises( ValueError ):
        p.to_polars( d )
#/def test_repeat_on_values_random_raises


def test_repeat_on_step_search_raises() -> None:
    p = _make_parser()
    d = { 'rho': { 'start': 0.1, 'step': 0.1, 'repeat': 'independent' } }
    with pytest.raises( ValueError ):
        p.to_polars( d )
#/def test_repeat_on_step_search_raises


# -- values_random / values_search (literal candidate lists)

def test_values_random_scalar_dtype_round_trips() -> None:
    p = _make_parser()
    d = { 'method': { 'values_random': [ 'a', 'b', 'c' ] } }
    df, _ = p.to_polars( d )
    row = df.filter( ( pl.col( 'par_name' ) == 'method' ) & ( pl.col( 'par_feature' ) == 'value' ) )
    assert row.shape[ 0 ] == 1
    assert row[ 'par_type' ][ 0 ] == 'random'
    assert list( row[ 'par_val_list_string' ][ 0 ] ) == [ 'a', 'b', 'c' ]
#/def test_values_random_scalar_dtype_round_trips


def test_values_search_scalar_dtype_round_trips() -> None:
    p = _make_parser()
    d = { 'method': { 'values_search': [ 'a', 'b', 'c' ] } }
    df, _ = p.to_polars( d )
    row = df.filter( ( pl.col( 'par_name' ) == 'method' ) & ( pl.col( 'par_feature' ) == 'value' ) )
    assert row.shape[ 0 ] == 1
    assert row[ 'par_type' ][ 0 ] == 'search'
    assert list( row[ 'par_val_list_string' ][ 0 ] ) == [ 'a', 'b', 'c' ]
#/def test_values_search_scalar_dtype_round_trips


def test_values_random_list_dtype_round_trips() -> None:
    p = _make_parser()
    p.register( 'modes_magnitude', pl.List( pl.Float64 ) )
    d = { 'modes_magnitude': { 'values_random': [ [ 1.0, 2.0 ], [ 3.0, 4.0 ] ] } }
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
    d = { 'rho': { 'values_random': [ 1, 2, 3 ] } }
    df, _ = p.to_polars( d )
    row = df.filter( ( pl.col( 'par_name' ) == 'rho' ) & ( pl.col( 'par_feature' ) == 'value' ) )
    assert list( row[ 'par_val_list_float64' ][ 0 ] ) == [ 1.0, 2.0, 3.0 ]
#/def test_values_random_casts_int_to_float


def test_values_random_rejects_wrong_type() -> None:
    p = _make_parser()
    d = { 'a': { 'values_random': [ 'x', 'y' ] } }
    with pytest.raises( ValueError ):
        p.to_polars( d )
#/def test_values_random_rejects_wrong_type


def test_values_random_rejects_lossy_float() -> None:
    p = _make_parser()
    d = { 'a': { 'values_random': [ 1.5 ] } }
    with pytest.raises( ValueError ):
        p.to_polars( d )
#/def test_values_random_rejects_lossy_float


def test_values_random_rejects_empty_list() -> None:
    p = _make_parser()
    d = { 'rho': { 'values_random': [] } }
    with pytest.raises( ValueError ):
        p.to_polars( d )
#/def test_values_random_rejects_empty_list


def test_values_random_unsupported_dtype_raises() -> None:
    p = _make_parser()
    p.register( 'standardize_y', pl.Boolean )
    d = { 'standardize_y': { 'values_random': [ True, False ] } }
    with pytest.raises( ValueError ):
        p.to_polars( d )
#/def test_values_random_unsupported_dtype_raises


# -- op_ctor_kwargs_forest (__table_ops_parameters__ -> TableOp ctor kwargs)

def _op_params_parser() -> ParameterParser:
    p = ParameterParser()
    p.register( 'op_idn',    pl.Utf8 )
    p.register( 'threshold', pl.Float64 )
    p.register( 'label',     pl.Utf8 )
    return p
#/def _op_params_parser


def test_op_ctor_kwargs_forest_empty_forest_returns_empty_dict() -> None:
    p = _op_params_parser()
    assert op_ctor_kwargs_forest( pl.DataFrame( schema = p.schema ) ) == {}
#/def test_op_ctor_kwargs_forest_empty_forest_returns_empty_dict


def test_op_ctor_kwargs_forest_extracts_one_op() -> None:
    p = _op_params_parser()
    tree, _ = p.to_polars( { 'op_idn': 'reader', 'threshold': 0.5, 'label': 'x' } )
    assert op_ctor_kwargs_forest( tree ) == { 'reader': { 'threshold': 0.5, 'label': 'x' } }
#/def test_op_ctor_kwargs_forest_extracts_one_op


def test_op_ctor_kwargs_forest_extracts_multiple_ops_via_threaded_node_idx() -> None:
    # Every parameterized op is told apart by node_idx -- so node_idx must
    # actually be threaded (node_idx_start in, next returned out) across
    # sibling calls, same as configTree.py does across [[parameters]] entries.
    p = _op_params_parser()
    tree_a, next_idx = p.to_polars( { 'op_idn': 'reader', 'threshold': 0.5 } )
    tree_b, _ = p.to_polars(
        { 'op_idn': 'writer', 'label': 'out' },
        node_idx_start = next_idx,
    )
    forest = pl.concat( [ tree_a, tree_b ], how = 'vertical' )
    assert op_ctor_kwargs_forest( forest ) == {
        'reader': { 'threshold': 0.5 },
        'writer': { 'label': 'out' },
    }
#/def test_op_ctor_kwargs_forest_extracts_multiple_ops_via_threaded_node_idx


def test_op_ctor_kwargs_forest_rejects_colliding_node_idx_across_ops() -> None:
    # Without threading node_idx, two ops both landing on node_idx 0 is
    # ambiguous -- raise rather than silently merging/misattributing kwargs
    # between them.
    p = _op_params_parser()
    tree_a, _ = p.to_polars( { 'op_idn': 'reader', 'threshold': 0.5 } )
    tree_b, _ = p.to_polars( { 'op_idn': 'writer', 'label': 'out' } )
    forest = pl.concat( [ tree_a, tree_b ], how = 'vertical' )
    with pytest.raises( ValueError, match = 'multiple .*op_idn' ):
        op_ctor_kwargs_forest( forest )
#/def test_op_ctor_kwargs_forest_rejects_colliding_node_idx_across_ops


def test_op_ctor_kwargs_forest_rejects_non_fixed_sibling() -> None:
    # Ctor kwargs need concrete values -- a random/search sibling under the
    # same op_idn node is a downstream-consumer concern (hyperparameter
    # search), not something build_tableOps() can turn into a kwarg.
    p = _op_params_parser()
    tree, _ = p.to_polars( {
        'op_idn': 'reader', 'threshold': { 'values_search': [ 0.1, 0.5 ] },
    } )
    with pytest.raises( ValueError, match = 'only fixed values are supported' ):
        op_ctor_kwargs_forest( tree )
#/def test_op_ctor_kwargs_forest_rejects_non_fixed_sibling


def test_op_ctor_kwargs_forest_ignores_node_with_no_op_idn_leaf() -> None:
    # A root node with no 'op_idn' leaf isn't a TableOp parameter node --
    # forward-looking, not exercised by any producer today.
    p = _op_params_parser()
    tree, _ = p.to_polars( { 'threshold': 0.9 } )
    assert op_ctor_kwargs_forest( tree ) == {}
#/def test_op_ctor_kwargs_forest_ignores_node_with_no_op_idn_leaf


def test_op_ctor_kwargs_forest_rejects_nested_or_and() -> None:
    # No single concrete value can be chosen for a ctor kwarg across `_or`
    # alternatives -- reject rather than guess.
    p = _op_params_parser()
    tree, _ = p.to_polars( {
        'op_idn': 'reader',
        '_or': [ { 'threshold': 0.1 }, { 'threshold': 0.5 } ],
    } )
    with pytest.raises( ValueError, match = 'nested _or/_and structure' ):
        op_ctor_kwargs_forest( tree )
#/def test_op_ctor_kwargs_forest_rejects_nested_or_and


# -- parameter_group_to_tree (raw dict -> forest tree, no pre-registration)

def test_parameter_group_to_tree_has_no_par_group_column() -> None:
    tree, _ = parameter_group_to_tree( 'reader', { 'threshold': 0.5 } )
    assert 'par_group' not in tree.columns
    assert op_ctor_kwargs_forest( tree ) == { 'reader': { 'threshold': 0.5 } }
#/def test_parameter_group_to_tree_has_no_par_group_column


def test_parameter_group_to_tree_infers_dtypes_from_literal_values() -> None:
    tree, _ = parameter_group_to_tree( 'reader', { 'threshold': 0.5, 'label': 'x', 'n': 3 } )
    assert op_ctor_kwargs_forest( tree ) == { 'reader': { 'threshold': 0.5, 'label': 'x', 'n': 3 } }
#/def test_parameter_group_to_tree_infers_dtypes_from_literal_values


def test_parameter_group_to_tree_multiple_ops_thread_node_idx() -> None:
    # Each call registers dtypes independently -- concatenating fragments
    # whose value-column sets differ requires a relaxed/diagonal concat.
    # node_idx must be threaded across calls (same as configTree.py does
    # across [[parameters]] entries) since node_idx is what distinguishes
    # ops (no par_group column).
    tree_a, next_idx = parameter_group_to_tree( 'reader', { 'threshold': 0.5 } )
    tree_b, _        = parameter_group_to_tree( 'writer', { 'label': 'out' }, node_idx_start = next_idx )
    forest = pl.concat( [ tree_a, tree_b ], how = 'diagonal_relaxed' )
    assert op_ctor_kwargs_forest( forest ) == {
        'reader': { 'threshold': 0.5 },
        'writer': { 'label': 'out' },
    }
#/def test_parameter_group_to_tree_multiple_ops_thread_node_idx


def test_parameter_group_to_tree_supports_values_search_dtype_inference() -> None:
    tree, _ = parameter_group_to_tree(
        'reader', { 'threshold': { 'values_search': [ 0.1, 0.5 ] } },
    )
    with pytest.raises( ValueError, match = 'only fixed values are supported' ):
        op_ctor_kwargs_forest( tree )  # parses fine; rejected only at extraction
#/def test_parameter_group_to_tree_supports_values_search_dtype_inference


# -- subtree() (the downward counterpart to leaf_paths()'s branch-to-root walk)

def test_subtree_at_or_junction_returns_junction_plus_both_alternatives() -> None:
    p = _make_parser()
    d = { 'a': 1, '_or': [ { 'b': 2 }, { 'rho': 0.5 } ] }
    df, _ = p.to_polars( d )

    or_node = df.filter( pl.col( 'par_type' ) == '_or' )[ 'node_idx' ][ 0 ]
    sub = p.subtree( df, or_node )

    # the 'a' content node (a sibling of the _or junction, not a
    # descendant) must NOT be included
    assert 'a' not in sub[ 'par_name' ].drop_nulls().to_list()
    assert set( sub[ 'par_name' ].drop_nulls().to_list() ) == { 'b', 'rho' }
    assert sub.filter( pl.col( 'par_type' ) == '_or' ).shape[ 0 ] == 1
#/def test_subtree_at_or_junction_returns_junction_plus_both_alternatives


def test_subtree_at_leaf_node_returns_only_that_row() -> None:
    p = _make_parser()
    d = { 'a': 1, '_or': [ { 'b': 2 }, { 'rho': 0.5 } ] }
    df, _ = p.to_polars( d )

    b_node = df.filter( pl.col( 'par_name' ) == 'b' )[ 'node_idx' ][ 0 ]
    sub = p.subtree( df, b_node )
    assert sub[ 'par_name' ].drop_nulls().to_list() == [ 'b' ]
#/def test_subtree_at_leaf_node_returns_only_that_row


def test_subtree_at_root_returns_whole_tree() -> None:
    p = _make_parser()
    d = { 'a': 1, '_or': [ { 'b': 2 }, { 'rho': 0.5 } ] }
    df, _ = p.to_polars( d )

    root_node = df.filter( pl.col( 'par_name' ) == 'a' )[ 'node_idx' ][ 0 ]
    sub = p.subtree( df, root_node )
    assert sub.shape[ 0 ] == df.shape[ 0 ]
#/def test_subtree_at_root_returns_whole_tree


def test_subtree_nested_and_within_or_returns_and_and_its_children_only() -> None:
    p = _make_parser()
    d = { '_or': [ { '_and': [ { 'a': 1 }, { 'b': 2 } ] }, { 'rho': 0.5 } ] }
    df, _ = p.to_polars( d )

    and_node = df.filter( pl.col( 'par_type' ) == '_and' )[ 'node_idx' ][ 0 ]
    sub = p.subtree( df, and_node )
    assert set( sub[ 'par_name' ].drop_nulls().to_list() ) == { 'a', 'b' }
    assert 'rho' not in sub[ 'par_name' ].drop_nulls().to_list()
#/def test_subtree_nested_and_within_or_returns_and_and_its_children_only


def test_subtree_missing_node_idx_raises_keyerror() -> None:
    p = _make_parser()
    df, _ = p.to_polars( { 'a': 1 } )
    with pytest.raises( KeyError ):
        p.subtree( df, 999 )
#/def test_subtree_missing_node_idx_raises_keyerror


# -- branch() (the third primitive alongside subtree()/leaf_paths(): find
# the one node matching an ordered chain of (par_name, value) pairs,
# walking through '_or'/'_and' junctions transparently, and return every
# ancestor's own content plus that match's own subtree())

def _species_tree_parser() -> ParameterParser:
    p = _make_parser()
    p.register( 'actor_type',  pl.Utf8 )
    p.register( 'species_idn', pl.Utf8 )
    return p
#/def _species_tree_parser

# Mirrors duckpond's own world_0.json shape closely enough to exercise the
# same 'par_group=\"actor\" -> actor_type=\"plant\" -> species_idn=\"grass_0\"'
# walk through two levels of '_or' junction, plus a value ('a') shared by
# every branch and one ('b') shared by every plant species specifically.
_SPECIES_TREE_DICT = {
    'a': 1,
    '_or': [
        {
            'actor_type': 'plant', 'b': 2,
            '_or': [
                { 'species_idn': 'grass_0', 'rho': 0.1 },
                { 'species_idn': 'berry_0', 'rho': 0.2 },
            ],
        },
        { 'actor_type': 'sentient' },
    ],
}

def test_branch_walks_through_or_junctions_and_includes_ancestor_content() -> None:
    p = _species_tree_parser()
    df, _ = p.to_polars( _SPECIES_TREE_DICT )

    result = p.branch( df, [ ( 'actor_type', 'plant' ), ( 'species_idn', 'grass_0' ) ] )

    assert set( result[ 'par_name' ].drop_nulls().to_list() ) == { 'a', 'actor_type', 'b', 'species_idn', 'rho' }
    rho_row = result.filter( pl.col( 'par_name' ) == 'rho' ).row( 0, named = True )
    assert ParameterParser._content_value( rho_row ) == 0.1
#/def test_branch_walks_through_or_junctions_and_includes_ancestor_content


def test_branch_excludes_sibling_branches() -> None:
    """berry_0's own 'rho' (a different node_idx sharing the same
    par_name) and sentient's own 'actor_type'='sentient' row must not
    leak into grass_0's own branch."""
    p = _species_tree_parser()
    df, _ = p.to_polars( _SPECIES_TREE_DICT )

    result = p.branch( df, [ ( 'actor_type', 'plant' ), ( 'species_idn', 'grass_0' ) ] )

    rho_values = { ParameterParser._content_value( r ) for r in result.filter( pl.col( 'par_name' ) == 'rho' ).iter_rows( named = True ) }
    assert rho_values == { 0.1 }
    actor_type_values = { ParameterParser._content_value( r ) for r in result.filter( pl.col( 'par_name' ) == 'actor_type' ).iter_rows( named = True ) }
    assert actor_type_values == { 'plant' }
#/def test_branch_excludes_sibling_branches


def test_branch_includes_full_subtree_below_final_match() -> None:
    """A species_idn block with further nesting below it (unlike
    grass_0/berry_0 above, which are leaves) -- every descendant comes
    along, same as subtree() would give directly."""
    p = _species_tree_parser()
    p.register( 'variant', pl.Utf8 )
    d = {
        '_or': [
            {
                'species_idn': 'grass_0',
                '_and': [ { 'variant': 'tall' }, { 'variant': 'short' } ],
            },
        ],
    }
    df, _ = p.to_polars( d )

    result = p.branch( df, [ ( 'species_idn', 'grass_0' ) ] )
    assert sorted(
        ParameterParser._content_value( r )
        for r in result.filter( pl.col( 'par_name' ) == 'variant' ).iter_rows( named = True )
    ) == [ 'short', 'tall' ]
#/def test_branch_includes_full_subtree_below_final_match


def test_branch_is_idempotent() -> None:
    p = _species_tree_parser()
    df, _ = p.to_polars( _SPECIES_TREE_DICT )
    path = [ ( 'actor_type', 'plant' ), ( 'species_idn', 'grass_0' ) ]

    once  = p.branch( df, path )
    twice = p.branch( once, path )

    assert once.equals( twice )
#/def test_branch_is_idempotent


def test_branch_empty_path_returns_whole_tree() -> None:
    p = _species_tree_parser()
    df, _ = p.to_polars( _SPECIES_TREE_DICT )
    assert p.branch( df, [] ).equals( df )
#/def test_branch_empty_path_returns_whole_tree


def test_branch_raises_keyerror_for_unmatched_step() -> None:
    p = _species_tree_parser()
    df, _ = p.to_polars( _SPECIES_TREE_DICT )
    with pytest.raises( KeyError, match = 'mineral_0' ):
        p.branch( df, [ ( 'actor_type', 'plant' ), ( 'species_idn', 'mineral_0' ) ] )
#/def test_branch_raises_keyerror_for_unmatched_step


def test_branch_does_not_search_across_unrelated_branch() -> None:
    """species_idn only exists under the 'plant' branch -- starting the
    path at 'actor_type'='sentient' must not find it by searching outside
    that branch."""
    p = _species_tree_parser()
    df, _ = p.to_polars( _SPECIES_TREE_DICT )
    with pytest.raises( KeyError ):
        p.branch( df, [ ( 'actor_type', 'sentient' ), ( 'species_idn', 'grass_0' ) ] )
#/def test_branch_does_not_search_across_unrelated_branch


# -- schema.table_schemas['parameter_tree'] consolidation (schema.py is
# now the single source of truth -- par_type/.../node_idx/parent_idx/
# par_val_* naming was previously hand-duplicated here)

def test_fresh_parser_schema_matches_table_schemas_parameter_tree() -> None:
    assert ParameterParser().schema == table_schemas[ 'parameter_tree' ]
#/def test_fresh_parser_schema_matches_table_schemas_parameter_tree
