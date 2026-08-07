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
    _, params, next_node_idx = toml_config_tree( tmp_path / 'top.toml', node_idn='top' )
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
    tree, params, next_node_idx = toml_config_tree(
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
    _, params, next_node_idx = toml_config_tree( tmp_path / 'top.toml', node_idn='top' )
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
toml     = "{params_root}/reader_params.toml"
""" )
    _, params, _ = toml_config_tree(
        tmp_path / 'leaf.toml', node_idn='root',
        template_vars = { 'params_root': 'params_root' },
    )
    assert op_ctor_kwargs_forest( params ) == { 'reader': { 'threshold': 0.5 } }


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
    tree, params, _ = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
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
    tree, _, _ = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
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
    tree, _, _ = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
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
    tree, _, _ = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
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
    tree, _, _ = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
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
