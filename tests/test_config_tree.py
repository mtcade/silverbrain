#
#//  test_config_tree.py
#//  silverbrain
#//

"""
Tests for configTree.toml_config_tree: pure parsing of a cell TOML's
transitive [[children]] file references into a flat node_idn/parent_idn
dataframe, plus the TableOp construction-parameter forest sourced from
[[parameters]] entries -- inline, or via an external .toml/.json reference
(squishy-sleeping-snowflake.md §2). No live Cell construction.
"""

from pathlib import Path

import polars as pl
import pytest

from silverbrain.configTree import config_tree, toml_config_tree
from silverbrain.parameterParse import ParameterParser, op_ctor_kwargs_forest


def _write( path: Path, content: str ) -> Path:
    path.write_text( content )
    return path


def test_single_file_root_row( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', """\
[cell]
source = ["inp"]
target = ["out"]

[[processes]]
node_idn = "double"
source   = ["inp"]
target   = ["out"]
""" )
    tree, params, next_node_idx = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
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

[[processes]]
node_idn = "leaf_op"
source   = ["inp"]
target   = ["out"]
""" )
    _write( tmp_path / 'middle.toml', """\
[cell]

[[children]]
node_idn = "grandkid"
toml     = "grandchild.toml"
source   = ["mid_in"]
target   = ["mid_out"]
""" )
    _write( tmp_path / 'top.toml', """\
[cell]

[[children]]
node_idn = "kid"
toml     = "middle.toml"
source   = ["top_in"]
target   = ["top_out"]
""" )

    tree, params, _ = toml_config_tree( tmp_path / 'top.toml', node_idn='top' )
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
    tree, params, _ = config_tree( tmp_path / 'leaf.toml', node_idn='root' )
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

[[children]]
node_idn = "kid"
toml     = "child.yaml"
""" )
    with pytest.raises( ValueError, match="Unsupported config file format" ):
        config_tree( tmp_path / 'top.toml', node_idn='top' )


# -- [[parameters]]: inline, external .toml, external .json


def test_parameters_inline( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', """\
[cell]

[[processes]]
node_idn = "reader"

[[parameters]]
node_idn  = "reader"
threshold = 0.5
label     = "x"
""" )
    _, params, _ = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
    assert op_ctor_kwargs_forest( params ) == { 'reader': { 'threshold': 0.5, 'label': 'x' } }


def test_parameters_external_toml_reference( tmp_path: Path ) -> None:
    _write( tmp_path / 'reader_params.toml', 'threshold = 0.5\nlabel = "x"\n' )
    _write( tmp_path / 'leaf.toml', """\
[cell]

[[parameters]]
node_idn = "reader"
toml     = "reader_params.toml"
""" )
    _, params, _ = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
    assert op_ctor_kwargs_forest( params ) == { 'reader': { 'threshold': 0.5, 'label': 'x' } }


def test_parameters_external_json_reference( tmp_path: Path ) -> None:
    _write( tmp_path / 'reader_params.json', '{"threshold": 0.5, "label": "x"}' )
    _write( tmp_path / 'leaf.toml', """\
[cell]

[[parameters]]
node_idn = "reader"
json     = "reader_params.json"
""" )
    _, params, _ = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
    assert op_ctor_kwargs_forest( params ) == { 'reader': { 'threshold': 0.5, 'label': 'x' } }


def test_parameters_both_toml_and_json_raises( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', """\
[cell]

[[parameters]]
node_idn = "reader"
toml     = "a.toml"
json     = "a.json"
""" )
    with pytest.raises( ValueError, match="both 'toml' and 'json'" ):
        toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )


def test_parameters_inline_mixed_with_external_reference_raises( tmp_path: Path ) -> None:
    _write( tmp_path / 'params.toml', 'threshold = 0.5\n' )
    _write( tmp_path / 'leaf.toml', """\
[cell]

[[parameters]]
node_idn  = "reader"
toml      = "params.toml"
threshold = 0.9
""" )
    with pytest.raises( ValueError, match="mixes an external reference" ):
        toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )


def test_parameters_across_multiple_files_collected_into_one_forest( tmp_path: Path ) -> None:
    _write( tmp_path / 'child.toml', """\
[cell]

[[processes]]
node_idn = "writer"

[[parameters]]
node_idn = "writer"
label    = "out"
""" )
    _write( tmp_path / 'top.toml', """\
[cell]

[[processes]]
node_idn = "reader"

[[parameters]]
node_idn  = "reader"
threshold = 0.5

[[children]]
node_idn = "kid"
toml     = "child.toml"
""" )
    _, params, next_node_idx = toml_config_tree( tmp_path / 'top.toml', node_idn='top' )
    assert op_ctor_kwargs_forest( params ) == {
        'reader': { 'threshold': 0.5 },
        'writer': { 'label': 'out' },
    }
    # one [[parameters]] entry per file, each a flat (no _or/_and) group --
    # each contributes exactly one node_idx, and the counter is threaded
    # into the [[children]] recursion, so the two don't collide.
    assert next_node_idx == 2


def test_parameters_node_idx_start_is_threaded_and_returned( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', """\
[cell]

[[parameters]]
node_idn  = "reader"
threshold = 0.5
""" )
    tree, params, next_node_idx = toml_config_tree(
        tmp_path / 'leaf.toml', node_idn='root', node_idx_start = 100,
    )
    assert next_node_idx == 101  # one flat group -> one node_idx consumed
    # both rows in the group (the 'op_idn' leaf and 'threshold') share that
    # one node_idx -- see _paramGroup_to_tree.
    assert params.filter( pl.col( 'par_group' ) == 'reader' )[ 'node_idx' ].unique().to_list() == [ 100 ]
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

[[children]]
node_idn = "kid"
toml     = "child.toml"
""" )
    _, params, next_node_idx = toml_config_tree( tmp_path / 'top.toml', node_idn='top' )
    reader_idx = params.filter( pl.col( 'par_group' ) == 'reader' )[ 'node_idx' ].unique().to_list()
    writer_idx = params.filter( pl.col( 'par_group' ) == 'writer' )[ 'node_idx' ].unique().to_list()
    assert reader_idx == [ 0 ]
    assert writer_idx == [ 1 ]  # threaded from the parent's own counter, not reset to 0
    assert next_node_idx == 2


def test_parameters_template_vars_apply_to_external_reference( tmp_path: Path ) -> None:
    params_dir = tmp_path / 'params_root'
    params_dir.mkdir()
    _write( params_dir / 'reader_params.toml', 'threshold = 0.5\n' )
    _write( tmp_path / 'leaf.toml', """\
[cell]

[[parameters]]
node_idn = "reader"
toml     = "{params_root}/reader_params.toml"
""" )
    _, params, _ = toml_config_tree(
        tmp_path / 'leaf.toml', node_idn='root',
        template_vars = { 'params_root': 'params_root' },
    )
    assert op_ctor_kwargs_forest( params ) == { 'reader': { 'threshold': 0.5 } }
