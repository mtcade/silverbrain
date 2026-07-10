#
#//  test_parameterParse.py
#//  silverbrain
#

import polars as pl
import pytest

from silverbrain.parameterParse import ParameterParser


def _make_parser() -> ParameterParser:
    p = ParameterParser()
    p.register( 'a',   pl.Int64 )
    p.register( 'b',   pl.Int64 )
    p.register( 'rho', pl.Float64 )
    return p
#/def _make_parser


def _node_row( df: pl.DataFrame, node_idx: int ) -> dict:
    rows = df.filter( pl.col( 'node_idx' ) == node_idx ).to_dicts()
    assert rows, f"no row with node_idx={node_idx}"
    return rows[ 0 ]
#/def _node_row


# -- par_idx backward compatibility

def test_flat_dict_par_idx_unchanged() -> None:
    p  = _make_parser()
    df, next_par_idx, next_node_idx = p.to_polars( { 'X': { 'a': 1, 'b': 2 } } )
    assert sorted( df['par_idx'].unique().to_list() ) == [ 0 ]
    assert next_par_idx  == 1
    assert next_node_idx == 1
#/def test_flat_dict_par_idx_unchanged


def test_groups_par_idx_matches_pre_existing_numbering() -> None:
    # One sibling par_group crossed with a 2-alternative _groups: par_idx should
    # be assigned exactly as before node_idx/parent_idx existed.
    p  = _make_parser()
    d  = { 'X': { 'a': 1 }, '_groups': [ { 'X': { 'b': 2 } }, { 'X': { 'b': 3 } } ] }
    df, next_par_idx, _ = p.to_polars( d )
    assert sorted( df['par_idx'].unique().to_list() ) == [ 1, 2 ]
    assert next_par_idx == 3
#/def test_groups_par_idx_matches_pre_existing_numbering


# -- node_idx / parent_idx: basic shape

def test_flat_dict_single_node_null_parent() -> None:
    p  = _make_parser()
    df, _, _ = p.to_polars( { 'X': { 'a': 1, 'b': 2 } } )
    assert df['node_idx'].n_unique() == 1
    assert df['parent_idx'].is_null().all()
#/def test_flat_dict_single_node_null_parent


def test_groups_inside_par_group_chains_to_parent() -> None:
    # X's own _groups (nested inside the par_group dict) must parent to X's
    # own base-content node, not to null.
    p  = _make_parser()
    d  = { 'X': { 'a': 1, '_groups': [ { 'rho': 0.1 }, { 'rho': 0.2 } ] } }
    df, _, _ = p.to_polars( d )

    base_node_idx = df.filter( pl.col( 'par_name' ) == 'a' )['node_idx'][ 0 ]
    rho_rows      = df.filter( pl.col( 'par_name' ) == 'rho' )
    assert rho_rows.shape[ 0 ] == 2
    assert ( rho_rows['parent_idx'] == base_node_idx ).all()
    assert rho_rows['node_idx'].n_unique() == 2
    assert base_node_idx not in rho_rows['node_idx'].to_list()
#/def test_groups_inside_par_group_chains_to_parent


def test_groups_inside_par_group_nested_two_levels() -> None:
    # _groups nested inside _groups is fine as long as it's inside a
    # par_group's own dict the whole way down.
    p = _make_parser()
    d = {
        'X': {
            'a': 1,
            '_groups': [
                { '_groups': [ { 'rho': 0.1 }, { 'rho': 0.2 } ] },
                { 'rho': 0.3 },
            ],
        },
    }
    df, _, _ = p.to_polars( d )

    base_node_idx = df.filter( pl.col( 'par_name' ) == 'a' )['node_idx'][ 0 ]
    rho_rows      = df.filter( pl.col( 'par_name' ) == 'rho' )
    assert rho_rows.shape[ 0 ] == 3
    # every rho row (regardless of which nesting branch) parents directly to
    # X's own base node -- the intermediate pure-_groups wrapper mints no node.
    assert ( rho_rows['parent_idx'] == base_node_idx ).all()
    assert rho_rows['node_idx'].n_unique() == 3
#/def test_groups_inside_par_group_nested_two_levels


def test_sibling_par_groups_do_not_parent_each_other() -> None:
    # Xk and importances are independent siblings; neither should appear in
    # the other's parent chain.
    p  = _make_parser()
    d  = { 'X': { 'a': 1 }, 'Y': { 'b': 2 } }
    df, _, _ = p.to_polars( d )

    x_node = df.filter( pl.col( 'par_group' ) == 'X' )['node_idx'][ 0 ]
    y_node = df.filter( pl.col( 'par_group' ) == 'Y' )['node_idx'][ 0 ]
    assert x_node != y_node
    assert ( df.filter( pl.col( 'par_group' ) == 'X' )['parent_idx'] ).is_null().all()
    assert ( df.filter( pl.col( 'par_group' ) == 'Y' )['parent_idx'] ).is_null().all()
#/def test_sibling_par_groups_do_not_parent_each_other


# -- node_idx / parent_idx: no dangling references, walkable chains

def test_no_dangling_parent_references() -> None:
    p  = _make_parser()
    d  = {
        'X': { 'a': 1, '_groups': [ { 'rho': 0.1 }, { 'rho': 0.2 } ] },
        'Y': { 'b': 2 },
        '_groups': [
            { 'X': { 'a': 10 } },
            { 'X': { 'a': 20 } },
        ],
    }
    df, _, _ = p.to_polars( d )

    node_idxs   = set( df['node_idx'].to_list() )
    parent_idxs = set( v for v in df['parent_idx'].to_list() if v is not None )
    assert parent_idxs <= node_idxs
#/def test_no_dangling_parent_references


def test_walk_parent_chain_reconstructs_groups_path() -> None:
    p  = _make_parser()
    d  = {
        'X': { 'a': 1, '_groups': [ { 'rho': 0.1 }, { 'rho': 0.2 } ] },
        'Y': { 'b': 2 },
    }
    df, _, _ = p.to_polars( d )

    rho_row = df.filter( ( pl.col( 'par_name' ) == 'rho' ) & ( pl.col( 'par_val_float64' ) == 0.1 ) ).to_dicts()[ 0 ]
    chain = [ rho_row['node_idx'] ]
    parent = rho_row['parent_idx']
    while parent is not None:
        chain.append( parent )
        parent = _node_row( df, parent )['parent_idx']

    # rho (leaf) -> X's own base node -> root (null)
    assert len( chain ) == 2
    base_node_idx = df.filter( pl.col( 'par_name' ) == 'a' )['node_idx'][ 0 ]
    assert chain[ 1 ] == base_node_idx
#/def test_walk_parent_chain_reconstructs_groups_path


# -- disallowed: _groups nested directly inside another _groups

def test_groups_directly_nested_in_groups_raises() -> None:
    p = _make_parser()
    d = {
        '_groups': [
            { 'X': { 'a': 1 }, '_groups': [ { 'X': { 'a': 2 } } ] },
        ],
    }
    with pytest.raises( ValueError, match = '_groups cannot be nested directly' ):
        p.to_polars( d )
#/def test_groups_directly_nested_in_groups_raises


def test_groups_directly_nested_in_groups_raises_even_without_own_keys() -> None:
    p = _make_parser()
    d = {
        '_groups': [
            { '_groups': [ { 'X': { 'a': 1 } } ] },
        ],
    }
    with pytest.raises( ValueError, match = '_groups cannot be nested directly' ):
        p.to_polars( d )
#/def test_groups_directly_nested_in_groups_raises_even_without_own_keys


def test_groups_nested_inside_par_group_does_not_raise() -> None:
    # Sanity check: the restriction is specific to to_polars' own _groups
    # recursion, not to _groups nested inside a par_group's own dict.
    p = _make_parser()
    d = {
        'X': {
            'a': 1,
            '_groups': [
                { '_groups': [ { 'rho': 0.1 } ] },
            ],
        },
    }
    p.to_polars( d )  # should not raise
#/def test_groups_nested_inside_par_group_does_not_raise
