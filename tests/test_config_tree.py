#
#//  test_config_tree.py
#//  silverbrain
#//

"""
Tests for configTree.toml_config_tree: pure parsing of a cell TOML's
transitive kind='cell' [[nodes]] file references into a flat node_idn/parent_idn
dataframe, plus the TableOp construction-parameter forest sourced from
[[parameters]] entries -- inline, or via an external .toml/.json reference
(squishy-sleeping-snowflake.md §2). No live Cell construction.
"""

from pathlib import Path

import polars as pl
import pytest

from silverbrain.bdd import Atom, BDDManager, ColVar, Union
from silverbrain.configTree import config_tree, toml_config_tree
from silverbrain.exprParse import parse_expr
from silverbrain.parameterParse import TABLE_OPS_PARAMETERS_GROUP, ParameterParser, op_ctor_kwargs_forest


def _write( path: Path, content: str ) -> Path:
    path.write_text( content )
    return path


def _node_idx_for_op( params: pl.DataFrame, op_idn: str ) -> int:
    """The node_idx of the 'op_idn' leaf row identifying op_idn -- since all
    parameterized ops share one par_group (TABLE_OPS_PARAMETERS_GROUP),
    node_idx is what actually distinguishes them."""
    val_cols = [ c for c in params.columns if c.startswith( 'par_val_' ) ]
    for row in params.filter(
        ( pl.col( 'par_group' ) == TABLE_OPS_PARAMETERS_GROUP ) & ( pl.col( 'par_name' ) == 'op_idn' )
    ).to_dicts():
        value = next( ( row[ c ] for c in val_cols if row[ c ] is not None ), None )
        if value == op_idn:
            return row[ 'node_idx' ]
    raise AssertionError( f"no op_idn leaf found for {op_idn!r}" )


def test_single_file_root_row( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', """\
[cell]
source = ["inp"]
target = ["out"]

[[nodes]]
node_idn = "double"
kind     = "op"
source   = ["inp"]
target   = ["out"]
""" )
    tree, params, next_node_idx, _ = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
    rows = { r[ 'node_idn' ]: r for r in tree.to_dicts() }

    assert next_node_idx == 0  # no [[parameters]] entries -> counter untouched

    assert rows[ 'root' ][ 'kind' ]       == 'cell'
    assert rows[ 'root' ][ 'parent_idn' ] is None
    assert rows[ 'root' ][ 'path' ]       == str( ( tmp_path / 'leaf.toml' ).resolve() )

    assert rows[ 'root' ][ 'cell_source' ] == [ 'inp' ]
    assert rows[ 'root' ][ 'cell_target' ] == [ 'out' ]

    assert rows[ 'double' ][ 'kind' ]       == 'op'
    assert rows[ 'double' ][ 'parent_idn' ] == 'root'
    assert rows[ 'double' ][ 'source' ]     == [ 'inp' ]
    assert rows[ 'double' ][ 'target' ]     == [ 'out' ]

    assert params.is_empty()
    assert params.schema == ParameterParser().schema


def test_nested_children_multi_level_parent_idn( tmp_path: Path ) -> None:
    _write( tmp_path / 'grandchild.toml', """\
[cell]
source = ["inp"]
target = ["out"]

[[nodes]]
node_idn = "leaf_op"
kind     = "op"
source   = ["inp"]
target   = ["out"]
""" )
    _write( tmp_path / 'middle.toml', """\
[cell]

[[nodes]]
node_idn = "grandkid"
kind     = "cell"
toml     = "grandchild.toml"
source   = ["mid_in"]
target   = ["mid_out"]
""" )
    _write( tmp_path / 'top.toml', """\
[cell]

[[nodes]]
node_idn = "kid"
kind     = "cell"
toml     = "middle.toml"
source   = ["top_in"]
target   = ["top_out"]
""" )

    tree, params, _, _ = toml_config_tree( tmp_path / 'top.toml', node_idn='top' )
    rows = { r[ 'node_idn' ]: r for r in tree.to_dicts() }

    assert set( rows.keys() ) == { 'top', 'kid', 'grandkid', 'leaf_op' }
    assert rows[ 'top' ][ 'parent_idn' ]      is None
    assert rows[ 'kid' ][ 'parent_idn' ]      == 'top'
    assert rows[ 'grandkid' ][ 'parent_idn' ] == 'kid'
    assert rows[ 'leaf_op' ][ 'parent_idn' ]  == 'grandkid'
    assert rows[ 'kid' ][ 'source' ]          == [ 'top_in' ]
    assert rows[ 'grandkid' ][ 'source' ]     == [ 'mid_in' ]

    assert params.is_empty()


# -- config_tree(): format dispatch by suffix


def test_config_tree_dispatches_toml_to_toml_parser( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', "[cell]\n" )
    tree, params, _, _ = config_tree( tmp_path / 'leaf.toml', node_idn='root' )
    assert tree[ 'node_idn' ].to_list() == [ 'root' ]
    assert params.is_empty()


def test_config_tree_rejects_unsupported_root_suffix( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.yaml', "cell: {}\n" )
    with pytest.raises( ValueError, match="Unsupported config file format" ):
        config_tree( tmp_path / 'leaf.yaml', node_idn='root' )


def test_format_inferred_at_each_step_child_suffix_checked_too( tmp_path: Path ) -> None:
    """A child referenced by a valid .toml parent still gets its own suffix
    check -- format inference happens at every node, not just the root."""
    _write( tmp_path / 'child.yaml', "cell: {}\n" )
    _write( tmp_path / 'top.toml', """\
[cell]

[[nodes]]
node_idn = "kid"
kind     = "cell"
toml     = "child.yaml"
""" )
    with pytest.raises( ValueError, match="Unsupported config file format" ):
        config_tree( tmp_path / 'top.toml', node_idn='top' )


# -- [[parameters]]: inline, external .toml, external .json


def test_parameters_inline( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', """\
[cell]

[[nodes]]
node_idn = "reader"
kind     = "op"

[[parameters]]
node_idn  = "reader"
threshold = 0.5
label     = "x"
""" )
    _, params, _, _ = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
    assert op_ctor_kwargs_forest( params ) == { 'reader': { 'threshold': 0.5, 'label': 'x' } }


def test_parameters_external_toml_reference( tmp_path: Path ) -> None:
    _write( tmp_path / 'reader_params.toml', 'threshold = 0.5\nlabel = "x"\n' )
    _write( tmp_path / 'leaf.toml', """\
[cell]

[[parameters]]
node_idn = "reader"
_toml    = "reader_params.toml"
""" )
    _, params, _, _ = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
    assert op_ctor_kwargs_forest( params ) == { 'reader': { 'threshold': 0.5, 'label': 'x' } }


def test_parameters_external_json_reference( tmp_path: Path ) -> None:
    _write( tmp_path / 'reader_params.json', '{"threshold": 0.5, "label": "x"}' )
    _write( tmp_path / 'leaf.toml', """\
[cell]

[[parameters]]
node_idn = "reader"
_json    = "reader_params.json"
""" )
    _, params, _, _ = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
    assert op_ctor_kwargs_forest( params ) == { 'reader': { 'threshold': 0.5, 'label': 'x' } }


def test_parameters_both_toml_and_json_raises( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', """\
[cell]

[[parameters]]
node_idn = "reader"
_toml    = "a.toml"
_json    = "a.json"
""" )
    with pytest.raises( ValueError, match="both '_toml' and '_json'" ):
        toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )


def test_parameters_inline_mixed_with_external_reference_raises( tmp_path: Path ) -> None:
    _write( tmp_path / 'params.toml', 'threshold = 0.5\n' )
    _write( tmp_path / 'leaf.toml', """\
[cell]

[[parameters]]
node_idn  = "reader"
_toml     = "params.toml"
threshold = 0.9
""" )
    with pytest.raises( ValueError, match="mixes an external reference" ):
        toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )


def test_parameters_across_multiple_files_collected_into_one_forest( tmp_path: Path ) -> None:
    _write( tmp_path / 'child.toml', """\
[cell]

[[nodes]]
node_idn = "writer"
kind     = "op"

[[parameters]]
node_idn = "writer"
label    = "out"
""" )
    _write( tmp_path / 'top.toml', """\
[cell]

[[nodes]]
node_idn = "reader"
kind     = "op"

[[parameters]]
node_idn  = "reader"
threshold = 0.5

[[nodes]]
node_idn = "kid"
kind     = "cell"
toml     = "child.toml"
""" )
    _, params, next_node_idx, _ = toml_config_tree( tmp_path / 'top.toml', node_idn='top' )
    assert op_ctor_kwargs_forest( params ) == {
        'reader': { 'threshold': 0.5 },
        'writer': { 'label': 'out' },
    }
    # one [[parameters]] entry per file, each a flat (no _or/_and) group --
    # each contributes exactly one node_idx, and the counter is threaded
    # into the kind='cell' recursion, so the two don't collide.
    assert next_node_idx == 2


def test_parameters_node_idx_start_is_threaded_and_returned( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', """\
[cell]

[[parameters]]
node_idn  = "reader"
threshold = 0.5
""" )
    tree, params, next_node_idx, _ = toml_config_tree(
        tmp_path / 'leaf.toml', node_idn='root', node_idx_start = 100,
    )
    assert next_node_idx == 101  # one flat group -> one node_idx consumed
    # both rows in the group (the 'op_idn' leaf and 'threshold') share that
    # one node_idx -- see _paramGroup_to_tree. All ops share one par_group
    # (TABLE_OPS_PARAMETERS_GROUP) now, so node_idx is what distinguishes them.
    assert params.filter(
        pl.col( 'par_group' ) == TABLE_OPS_PARAMETERS_GROUP
    )[ 'node_idx' ].unique().to_list() == [ 100 ]
    assert op_ctor_kwargs_forest( params ) == { 'reader': { 'threshold': 0.5 } }


def test_parameters_across_files_get_disjoint_node_idx_when_threaded( tmp_path: Path ) -> None:
    _write( tmp_path / 'child.toml', """\
[cell]

[[parameters]]
node_idn = "writer"
label    = "out"
""" )
    _write( tmp_path / 'top.toml', """\
[cell]

[[parameters]]
node_idn  = "reader"
threshold = 0.5

[[nodes]]
node_idn = "kid"
kind     = "cell"
toml     = "child.toml"
""" )
    _, params, next_node_idx, _ = toml_config_tree( tmp_path / 'top.toml', node_idn='top' )
    reader_idx = _node_idx_for_op( params, 'reader' )
    writer_idx = _node_idx_for_op( params, 'writer' )
    assert reader_idx == 0
    assert writer_idx == 1  # threaded from the parent's own counter, not reset to 0
    assert next_node_idx == 2


def test_parameters_template_vars_apply_to_external_reference( tmp_path: Path ) -> None:
    params_dir = tmp_path / 'params_root'
    params_dir.mkdir()
    _write( params_dir / 'reader_params.toml', 'threshold = 0.5\n' )
    _write( tmp_path / 'leaf.toml', """\
[cell]

[[parameters]]
node_idn = "reader"
_toml    = "{params_root}/reader_params.toml"
""" )
    _, params, _, _ = toml_config_tree(
        tmp_path / 'leaf.toml', node_idn='root',
        template_vars = { 'params_root': 'params_root' },
    )
    assert op_ctor_kwargs_forest( params ) == { 'reader': { 'threshold': 0.5 } }


# -- [resources] / [nodes.resources]: own defaults vs. per-instantiation
# override, collected into resources_map (node_idn -> {'own', 'override'}),
# NOT flattened into one frame the way [[parameters]] is -- see
# configTree.py's module docstring.


def test_resources_own_defaults_inline_items( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', """\
[cell]

[[resources.items]]
table_idn = "foo"
path      = "/data/foo.parquet"
""" )
    _, _, _, resources_map = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
    own = resources_map[ 'root' ][ 'own' ]
    assert own.row( 0, named = True )[ 'table_idn' ] == 'foo'
    assert own.row( 0, named = True )[ 'path' ] == '/data/foo.parquet'
    assert resources_map[ 'root' ].get( 'override' ) is None


def test_resources_own_defaults_external_json_reference( tmp_path: Path ) -> None:
    _write( tmp_path / 'resources.json', '{"items": [{"table_idn": "foo", "path": "/data/foo.parquet"}]}' )
    _write( tmp_path / 'leaf.toml', """\
[cell]

[resources]
_json = "resources.json"
""" )
    _, _, _, resources_map = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
    own = resources_map[ 'root' ][ 'own' ]
    assert own.row( 0, named = True )[ 'table_idn' ] == 'foo'


def test_resources_no_block_yields_no_own_entry( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', "[cell]\n" )
    _, _, _, resources_map = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
    assert resources_map[ 'root' ][ 'own' ] is None


def test_resources_both_json_and_items_raises( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', """\
[cell]

[resources]
_json = "resources.json"

[[resources.items]]
table_idn = "foo"
path      = "/data/foo.parquet"
""" )
    with pytest.raises( ValueError, match="both '_json' and 'items'" ):
        toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )


def test_resources_unrecognized_key_raises( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', """\
[resources]
bogus = "x"
""" )
    with pytest.raises( ValueError, match="unrecognized key" ):
        toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )


def test_resources_template_vars_apply_to_external_reference( tmp_path: Path ) -> None:
    resources_dir = tmp_path / 'resources_root'
    resources_dir.mkdir()
    _write( resources_dir / 'resources.json', '{"items": [{"table_idn": "foo", "path": "/data/foo.parquet"}]}' )
    _write( tmp_path / 'leaf.toml', """\
[cell]

[resources]
_json = "{resources_root}/resources.json"
""" )
    _, _, _, resources_map = toml_config_tree(
        tmp_path / 'leaf.toml', node_idn='root',
        template_vars = { 'resources_root': 'resources_root' },
    )
    assert resources_map[ 'root' ][ 'own' ].row( 0, named = True )[ 'table_idn' ] == 'foo'


def test_resources_inline_items_template_vars_substituted( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', """\
[cell]

[[resources.items]]
table_idn = "foo"
path      = "{bundle_root}/foo.parquet"
""" )
    _, _, _, resources_map = toml_config_tree(
        tmp_path / 'leaf.toml', node_idn='root',
        template_vars = { 'bundle_root': '/bundles/train_0' },
    )
    assert resources_map[ 'root' ][ 'own' ].row( 0, named = True )[ 'path' ] == '/bundles/train_0/foo.parquet'


def test_nodes_resources_override_collected_into_dict( tmp_path: Path ) -> None:
    _write( tmp_path / 'child.toml', """\
[cell]

[[resources.items]]
table_idn = "foo"
path      = "/child/default/foo.parquet"
""" )
    _write( tmp_path / 'top.toml', """\
[cell]

[[nodes]]
node_idn = "kid"
kind     = "cell"
toml     = "child.toml"
[nodes.resources]
[[nodes.resources.items]]
table_idn = "foo"
path      = "/top/override/foo.parquet"
""" )
    _, _, _, resources_map = toml_config_tree( tmp_path / 'top.toml', node_idn='top' )
    # 'own' (child's own file's default) and 'override' (parent's binding
    # for that instantiation) are distinct slots under the SAME node_idn.
    assert resources_map[ 'kid' ][ 'own' ].row( 0, named = True )[ 'path' ] == '/child/default/foo.parquet'
    assert resources_map[ 'kid' ][ 'override' ].row( 0, named = True )[ 'path' ] == '/top/override/foo.parquet'


def test_nodes_resources_on_op_kind_raises( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', """\
[cell]

[[nodes]]
node_idn = "reader"
kind     = "op"
[nodes.resources]
[[nodes.resources.items]]
table_idn = "foo"
path      = "/x"
""" )
    with pytest.raises( ValueError, match="declares 'resources' but kind='op'" ):
        toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )


def test_nodes_resources_on_placeholder_kind_raises( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', """\
[cell]

[[nodes]]
node_idn = "ph"
kind     = "placeholder"
[nodes.resources]
[[nodes.resources.items]]
table_idn = "foo"
path      = "/x"
""" )
    with pytest.raises( ValueError, match="declares 'resources' but kind='placeholder'" ):
        toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )


def test_resources_map_merges_up_through_multi_level_nesting( tmp_path: Path ) -> None:
    """Two sibling kind='cell' nodes instantiating the SAME child template
    (child.toml), each with a distinct [nodes.resources] override for the
    same table_idn -- proves resources_map keeps them independent (the core
    scenario motivating per-instantiation binding over threaded/child-wins
    resources)."""
    _write( tmp_path / 'child.toml', "[cell]\n" )
    _write( tmp_path / 'top.toml', """\
[cell]

[[nodes]]
node_idn = "kid_a"
kind     = "cell"
toml     = "child.toml"
[nodes.resources]
[[nodes.resources.items]]
table_idn = "shared_name"
path      = "/a/data.parquet"

[[nodes]]
node_idn = "kid_b"
kind     = "cell"
toml     = "child.toml"
[nodes.resources]
[[nodes.resources.items]]
table_idn = "shared_name"
path      = "/b/data.parquet"
""" )
    _, _, _, resources_map = toml_config_tree( tmp_path / 'top.toml', node_idn='top' )
    assert resources_map[ 'kid_a' ][ 'override' ].row( 0, named = True )[ 'path' ] == '/a/data.parquet'
    assert resources_map[ 'kid_b' ][ 'override' ].row( 0, named = True )[ 'path' ] == '/b/data.parquet'
    # Neither child declared its own [resources] -- 'own' stays None, not
    # cross-contaminated with the other sibling's override.
    assert resources_map[ 'kid_a' ][ 'own' ] is None
    assert resources_map[ 'kid_b' ][ 'own' ] is None


# -- kind="placeholder"


def test_placeholder_node_carries_columns_as_expr_text( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', """\
[cell]

[[nodes]]
node_idn = "future"
kind     = "placeholder"
source   = ["inp"]
target   = ["out"]
columns  = { a = "Int64", b = "Utf8" }
""" )
    tree, params, _, _ = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
    row = tree.filter( pl.col( 'node_idn' ) == 'future' ).row( 0, named = True )

    assert row[ 'kind' ]       == 'placeholder'
    assert row[ 'source' ]     == [ 'inp' ]
    assert row[ 'target' ]     == [ 'out' ]

    m = BDDManager()
    expr = parse_expr( row[ 'placeholder_columns' ] )
    assert expr.to_bdd( m ) is Union( Atom( 'a', pl.Int64 ), Atom( 'b', pl.Utf8 ) ).to_bdd( m )


def test_placeholder_node_defaults_to_empty_columns( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', """\
[cell]

[[nodes]]
node_idn = "future"
kind     = "placeholder"
""" )
    tree, _, _, _ = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
    row = tree.filter( pl.col( 'node_idn' ) == 'future' ).row( 0, named = True )
    m = BDDManager()
    from silverbrain.bdd import FALSE
    assert parse_expr( row[ 'placeholder_columns' ] ).to_bdd( m ) is FALSE


def test_placeholder_node_dict_form_infers_via_sentinel( tmp_path: Path ) -> None:
    """A dict value of "?" means "infer from schema.cannonical_schema" --
    same sentinel spelling exprParse.py's text grammar uses."""
    _write( tmp_path / 'leaf.toml', """\
[cell]

[[nodes]]
node_idn = "future"
kind     = "placeholder"
columns  = { op_idn = "?" }
""" )
    tree, _, _, _ = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
    row = tree.filter( pl.col( 'node_idn' ) == 'future' ).row( 0, named = True )

    from silverbrain.schema import cannonical_schema
    m = BDDManager()
    expr = parse_expr( row[ 'placeholder_columns' ] )
    assert expr.to_bdd( m ) is Atom( 'op_idn', cannonical_schema[ 'op_idn' ] ).to_bdd( m )


def test_placeholder_node_text_form_with_colvar( tmp_path: Path ) -> None:
    """The new ColVar-capable interface form -- the main use case: a saved
    map with a placeholder whose interface references a column variable,
    to be resolved later once the placeholder is filled in."""
    _write( tmp_path / 'leaf.toml', """\
[cell]

[[nodes]]
node_idn = "future"
kind     = "placeholder"
columns  = "Union(ColVar('C'), Atom('nu_col_0', Int64))"
""" )
    tree, _, _, _ = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
    row = tree.filter( pl.col( 'node_idn' ) == 'future' ).row( 0, named = True )

    m = BDDManager()
    expr = parse_expr( row[ 'placeholder_columns' ] )
    expected = Union( ColVar( 'C' ), Atom( 'nu_col_0', pl.Int64 ) )
    assert expr.to_bdd( m ) is expected.to_bdd( m )


def test_op_and_cell_nodes_have_null_placeholder_columns( tmp_path: Path ) -> None:
    _write( tmp_path / 'child.toml', "[cell]\n" )
    _write( tmp_path / 'leaf.toml', """\
[cell]

[[nodes]]
node_idn = "an_op"
kind     = "op"

[[nodes]]
node_idn = "a_cell"
kind     = "cell"
toml     = "child.toml"
""" )
    tree, _, _, _ = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
    rows = { r[ 'node_idn' ]: r for r in tree.to_dicts() }
    assert rows[ 'an_op' ][ 'placeholder_columns' ] is None
    assert rows[ 'a_cell' ][ 'placeholder_columns' ] is None


def test_unknown_kind_raises( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', """\
[cell]

[[nodes]]
node_idn = "mystery"
kind     = "bogus"
""" )
    with pytest.raises( ValueError, match="unknown kind" ):
        toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )


# -- kind="cell": cell_idn resolution (bundle.resolve_map_fp)


def test_cell_kind_resolves_via_cell_idn( tmp_path: Path ) -> None:
    data_root = tmp_path / 'data_root'
    ( data_root / 'cells' ).mkdir( parents=True )
    _write( data_root / 'cells_index.toml', """\
[[cells]]
cell_idn = "child_cell"
path     = "cells/child.toml"
""" )
    _write( data_root / 'cells' / 'child.toml', "[cell]\n" )
    _write( tmp_path / 'parent.toml', """\
[cell]

[[nodes]]
node_idn = "kid"
kind     = "cell"
cell_idn = "child_cell"
""" )
    tree, _, _, _ = toml_config_tree(
        tmp_path / 'parent.toml', node_idn='root',
        app_name='nonexistent_test_app',
        fallback_data_roots={ 'data_root': data_root },
    )
    rows = { r[ 'node_idn' ]: r for r in tree.to_dicts() }
    assert rows[ 'kid' ][ 'path' ] == str( ( data_root / 'cells' / 'child.toml' ).resolve() )


def test_cell_kind_both_toml_and_cell_idn_raises( tmp_path: Path ) -> None:
    _write( tmp_path / 'parent.toml', """\
[cell]

[[nodes]]
node_idn = "kid"
kind     = "cell"
toml     = "child.toml"
cell_idn = "child_cell"
""" )
    with pytest.raises( ValueError, match="both 'toml' and 'cell_idn'" ):
        toml_config_tree( tmp_path / 'parent.toml', node_idn='root' )


def test_cell_kind_cell_idn_without_app_name_raises( tmp_path: Path ) -> None:
    _write( tmp_path / 'parent.toml', """\
[cell]

[[nodes]]
node_idn = "kid"
kind     = "cell"
cell_idn = "child_cell"
""" )
    with pytest.raises( ValueError, match="no app_name was supplied" ):
        toml_config_tree( tmp_path / 'parent.toml', node_idn='root' )


def test_cell_kind_neither_toml_nor_cell_idn_raises( tmp_path: Path ) -> None:
    _write( tmp_path / 'parent.toml', """\
[cell]

[[nodes]]
node_idn = "kid"
kind     = "cell"
""" )
    with pytest.raises( ValueError, match="neither 'toml' nor 'cell_idn'" ):
        toml_config_tree( tmp_path / 'parent.toml', node_idn='root' )


# -- op_type / anonymous node_idn


def test_op_node_idn_is_optional_and_auto_generated( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', """\
[[nodes]]
kind     = "op"
op_type  = "make_constant"
source   = ["trig"]
target   = ["out"]
""" )
    tree, _, _, _ = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
    op_rows = tree.filter( pl.col( 'kind' ) == 'op' ).to_dicts()
    assert len( op_rows ) == 1
    assert op_rows[ 0 ][ 'node_idn' ].startswith( 'make_constant__' )
    assert op_rows[ 0 ][ 'op_type' ] == 'make_constant'


def test_op_type_defaults_to_node_idn_when_omitted( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', """\
[[nodes]]
node_idn = "reader"
kind     = "op"
""" )
    tree, _, _, _ = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
    row = tree.filter( pl.col( 'node_idn' ) == 'reader' ).row( 0, named = True )
    assert row[ 'op_type' ] == 'reader'


def test_two_anonymous_nodes_share_op_type_with_distinct_node_idn( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', """\
[[nodes]]
kind    = "op"
op_type = "make_constant"
source  = ["trig"]
target  = ["a"]

[[nodes]]
kind    = "op"
op_type = "make_constant"
source  = ["trig"]
target  = ["b"]
""" )
    tree, _, _, _ = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
    op_rows = tree.filter( pl.col( 'kind' ) == 'op' ).to_dicts()
    assert len( op_rows ) == 2
    node_idns = { row[ 'node_idn' ] for row in op_rows }
    assert len( node_idns ) == 2  # distinct, despite sharing op_type
    assert all( row[ 'op_type' ] == 'make_constant' for row in op_rows )


def test_duplicate_explicit_node_idn_raises( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', """\
[[nodes]]
node_idn = "dup"
kind     = "op"

[[nodes]]
node_idn = "dup"
kind     = "op"
""" )
    with pytest.raises( ValueError, match="duplicate node_idn='dup'" ):
        toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )


# -- join/join_k/round_origin (Token/round-id wiring model)


def test_join_join_k_default_to_all_none( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', """\
[[nodes]]
node_idn = "plain"
kind     = "op"
source   = ["a"]
target   = ["out"]
""" )
    tree, _, _, _ = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
    row = tree.filter( pl.col( 'node_idn' ) == 'plain' ).row( 0, named = True )
    assert row[ 'join' ]   == 'all'
    assert row[ 'join_k' ] is None


def test_join_any_threads_through( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', """\
[[nodes]]
node_idn = "merger"
kind     = "op"
op_type  = "merge"
join     = "any"
source   = ["a", "b"]
target   = ["out"]
""" )
    tree, _, _, _ = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
    row = tree.filter( pl.col( 'node_idn' ) == 'merger' ).row( 0, named = True )
    assert row[ 'join' ]   == 'any'
    assert row[ 'join_k' ] is None


def test_join_at_least_k_requires_valid_join_k( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', """\
[[nodes]]
node_idn = "quorum"
kind     = "op"
join     = "at_least_k"
join_k   = 2
source   = ["a", "b", "c"]
target   = ["out"]
""" )
    tree, _, _, _ = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
    row = tree.filter( pl.col( 'node_idn' ) == 'quorum' ).row( 0, named = True )
    assert row[ 'join' ]   == 'at_least_k'
    assert row[ 'join_k' ] == 2


def test_join_at_least_k_missing_join_k_raises( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', """\
[[nodes]]
node_idn = "quorum"
kind     = "op"
join     = "at_least_k"
source   = ["a", "b"]
target   = ["out"]
""" )
    with pytest.raises( ValueError, match="join_k" ):
        toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )


def test_join_k_without_at_least_k_raises( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', """\
[[nodes]]
node_idn = "plain"
kind     = "op"
join_k   = 1
source   = ["a"]
target   = ["out"]
""" )
    with pytest.raises( ValueError, match="join_k" ):
        toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )


def test_invalid_join_value_raises( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', """\
[[nodes]]
node_idn = "plain"
kind     = "op"
join     = "bogus"
source   = ["a"]
target   = ["out"]
""" )
    with pytest.raises( ValueError, match="join" ):
        toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )


def test_join_on_nested_cell_node_threads_through( tmp_path: Path ) -> None:
    _write( tmp_path / 'child.toml', """\
[cell]
source = ["a", "b"]
target = ["out"]
""" )
    _write( tmp_path / 'parent.toml', """\
[[nodes]]
node_idn = "kid"
kind     = "cell"
toml     = "child.toml"
join     = "any"
source   = ["a", "b"]
target   = ["out"]
""" )
    tree, _, _, _ = toml_config_tree( tmp_path / 'parent.toml', node_idn='root' )
    row = tree.filter( ( pl.col( 'node_idn' ) == 'kid' ) & ( pl.col( 'kind' ) == 'cell' ) ).row( 0, named = True )
    assert row[ 'join' ] == 'any'
