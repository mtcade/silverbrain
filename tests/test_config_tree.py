#
#//  test_config_tree.py
#//  silverbrain
#//

"""
Tests for configTree.toml_config_tree: pure parsing of a cell TOML's
transitive kind='cell' [[nodes]] file references into a flat node_idn/parent_idn
dataframe, plus the TableOp construction-parameter forest sourced from
[[parameters]] entries -- inline, or via an external .toml/.json reference.
No live Cell construction.
"""

from pathlib import Path

import polars as pl
import pytest

from silverbrain.bdd import Atom, BDDManager, ColVar, Union
from silverbrain.configTree import (
    config_tree, toml_config_tree,
    resolve_actor_declarations, resolve_resource_declarations,
)
from silverbrain.exprParse import parse_expr
from silverbrain.parameterParse import ParameterParser, op_ctor_kwargs_forest


def _write( path: Path, content: str ) -> Path:
    path.write_text( content )
    return path


def _node_idx_for_op( params: pl.DataFrame, op_idn: str ) -> int:
    """The node_idx of the 'op_idn' leaf row identifying op_idn -- node_idx
    is what distinguishes ops (no par_group column)."""
    val_cols = [ c for c in params.columns if c.startswith( 'par_val_' ) ]
    for row in params.filter( pl.col( 'par_name' ) == 'op_idn' ).to_dicts():
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
    # one node_idx -- see ParameterParser.to_polars(). node_idx is what
    # distinguishes ops (no par_group column).
    assert params[ 'node_idx' ].unique().to_list() == [ 100 ]
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


# -- variables.resources / variables.actors: path-addressed declarations,
# root-wins, resolved once over the whole tree by
# resolve_resource_declarations()/resolve_actor_declarations() -- see
# configTree.py's module docstring. Every row toml_config_tree() itself
# returns (declarations, unresolved) carries 'abs_path'/'declarer_depth';
# these tests check both the raw declaration rows AND, where relevant, the
# resolved dict.


def test_resources_own_defaults_inline_items( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', """\
[cell]

[[variables.resources]]
table_idn = "foo"
path      = "/data/foo.parquet"
""" )
    _, _, _, declarations = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
    resolved = resolve_resource_declarations( declarations[ 'resources' ] )
    assert resolved[ 'root' ].row( 0, named = True )[ 'table_idn' ] == 'foo'
    assert resolved[ 'root' ].row( 0, named = True )[ 'path' ] == '/data/foo.parquet'


def test_resources_no_block_yields_no_entry( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', "[cell]\n" )
    _, _, _, declarations = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
    assert resolve_resource_declarations( declarations[ 'resources' ] ) == {}


def test_resources_inline_items_template_vars_substituted( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', """\
[cell]

[[variables.resources]]
table_idn = "foo"
path      = "{bundle_root}/foo.parquet"
""" )
    _, _, _, declarations = toml_config_tree(
        tmp_path / 'leaf.toml', node_idn='root',
        template_vars = { 'bundle_root': '/bundles/train_0' },
    )
    resolved = resolve_resource_declarations( declarations[ 'resources' ] )
    assert resolved[ 'root' ].row( 0, named = True )[ 'path' ] == '/bundles/train_0/foo.parquet'


def test_variables_resources_target_addresses_direct_child( tmp_path: Path ) -> None:
    _write( tmp_path / 'child.toml', """\
[cell]

[[variables.resources]]
table_idn = "foo"
path      = "/child/default/foo.parquet"
""" )
    _write( tmp_path / 'top.toml', """\
[cell]

[[nodes]]
node_idn = "kid"
kind     = "cell"
toml     = "child.toml"

[[variables.resources]]
target    = "kid"
table_idn = "foo"
path      = "/top/override/foo.parquet"
""" )
    _, _, _, declarations = toml_config_tree( tmp_path / 'top.toml', node_idn='top' )
    resolved = resolve_resource_declarations( declarations[ 'resources' ] )
    # top's declaration (declarer_depth=0, the root) beats kid's own
    # (declarer_depth=1, deeper) -- root-wins.
    assert resolved[ 'top.kid' ].row( 0, named = True )[ 'path' ] == '/top/override/foo.parquet'


def test_variables_resources_reaches_multiple_levels_deep_without_forwarding( tmp_path: Path ) -> None:
    """An ancestor several kind='cell' hops above a node can bind straight
    to it -- NO intermediate file has to re-declare or forward anything."""
    _write( tmp_path / 'grandchild.toml', "[cell]\n" )
    _write( tmp_path / 'child.toml', """\
[cell]

[[nodes]]
node_idn = "grandkid"
kind     = "cell"
toml     = "grandchild.toml"
""" )
    _write( tmp_path / 'top.toml', """\
[cell]

[[nodes]]
node_idn = "kid"
kind     = "cell"
toml     = "child.toml"

[[variables.resources]]
target    = "kid.grandkid"
table_idn = "foo"
path      = "/top/reaches/deep.parquet"
""" )
    _, _, _, declarations = toml_config_tree( tmp_path / 'top.toml', node_idn='top' )
    resolved = resolve_resource_declarations( declarations[ 'resources' ] )
    assert resolved[ 'top.kid.grandkid' ].row( 0, named = True )[ 'path' ] == '/top/reaches/deep.parquet'
    # child.toml never mentioned resources at all -- nothing to forward.
    assert 'top.kid' not in resolved


def test_variables_resources_root_wins_over_intermediate_default( tmp_path: Path ) -> None:
    """An intermediate cell's own declaration for a descendant is only a
    reusable fallback -- a more senior ancestor's declaration for the same
    target always wins, even though the intermediate is "closer"."""
    _write( tmp_path / 'grandchild.toml', "[cell]\n" )
    _write( tmp_path / 'child.toml', """\
[cell]

[[nodes]]
node_idn = "grandkid"
kind     = "cell"
toml     = "grandchild.toml"

[[variables.resources]]
target    = "grandkid"
table_idn = "foo"
path      = "/child/default.parquet"
""" )
    _write( tmp_path / 'top.toml', """\
[cell]

[[nodes]]
node_idn = "kid"
kind     = "cell"
toml     = "child.toml"

[[variables.resources]]
target    = "kid.grandkid"
table_idn = "foo"
path      = "/top/wins.parquet"
""" )
    _, _, _, declarations = toml_config_tree( tmp_path / 'top.toml', node_idn='top' )
    resolved = resolve_resource_declarations( declarations[ 'resources' ] )
    assert resolved[ 'top.kid.grandkid' ].row( 0, named = True )[ 'path' ] == '/top/wins.parquet'


def test_variables_resources_sibling_instantiations_of_same_template_stay_independent( tmp_path: Path ) -> None:
    _write( tmp_path / 'child.toml', "[cell]\n" )
    _write( tmp_path / 'top.toml', """\
[cell]

[[nodes]]
node_idn = "kid_a"
kind     = "cell"
toml     = "child.toml"

[[nodes]]
node_idn = "kid_b"
kind     = "cell"
toml     = "child.toml"

[[variables.resources]]
target    = "kid_a"
table_idn = "shared_name"
path      = "/a/data.parquet"

[[variables.resources]]
target    = "kid_b"
table_idn = "shared_name"
path      = "/b/data.parquet"
""" )
    _, _, _, declarations = toml_config_tree( tmp_path / 'top.toml', node_idn='top' )
    resolved = resolve_resource_declarations( declarations[ 'resources' ] )
    assert resolved[ 'top.kid_a' ].row( 0, named = True )[ 'path' ] == '/a/data.parquet'
    assert resolved[ 'top.kid_b' ].row( 0, named = True )[ 'path' ] == '/b/data.parquet'


def test_variables_resources_multiple_table_idns_per_target_all_survive( tmp_path: Path ) -> None:
    """Resolution is per (target, table_idn), not whole-manifest replacement
    -- a target's resource manifest can hold more than one table, and an
    ancestor override of ONE table_idn doesn't drop the target's others."""
    _write( tmp_path / 'child.toml', """\
[cell]

[[variables.resources]]
table_idn = "foo"
path      = "/child/foo.parquet"

[[variables.resources]]
table_idn = "bar"
path      = "/child/bar.parquet"
""" )
    _write( tmp_path / 'top.toml', """\
[cell]

[[nodes]]
node_idn = "kid"
kind     = "cell"
toml     = "child.toml"

[[variables.resources]]
target    = "kid"
table_idn = "foo"
path      = "/top/foo.parquet"
""" )
    _, _, _, declarations = toml_config_tree( tmp_path / 'top.toml', node_idn='top' )
    resolved = resolve_resource_declarations( declarations[ 'resources' ] )
    by_table_idn = { row[ 'table_idn' ]: row[ 'path' ] for row in resolved[ 'top.kid' ].to_dicts() }
    assert by_table_idn == { 'foo': '/top/foo.parquet', 'bar': '/child/bar.parquet' }


# -- variables.actors: same path-addressing/root-wins mechanism, binding a
# real op_type (+ ctor kwargs) to a variable's abs_path instead of a
# resource manifest entry.


def test_variables_actors_binds_direct_variable( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', """\
[cell]

[[nodes]]
node_idn = "picker"
kind     = "variable"
columns  = { a = "Int64" }

[[variables.actors]]
target  = "picker"
op_type = "RandomPicker"
[variables.actors.parameters]
seed = 7
""" )
    _, _, _, declarations = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
    resolved = resolve_actor_declarations( declarations[ 'actors' ] )
    assert resolved[ 'root.picker' ][ 'op_type' ] == 'RandomPicker'
    assert resolved[ 'root.picker' ][ 'kwargs' ] == { 'seed': 7 }
    assert resolved[ 'root.picker' ][ 'declaring_path' ] == 'root'


def test_variables_actors_reaches_multiple_levels_deep_without_forwarding( tmp_path: Path ) -> None:
    _write( tmp_path / 'search.toml', """\
[cell]

[[nodes]]
node_idn = "parameter_pick_next"
kind     = "variable"
columns  = { a = "Int64" }
""" )
    _write( tmp_path / 'wrapper.toml', """\
[cell]

[[nodes]]
node_idn = "search"
kind     = "cell"
toml     = "search.toml"
""" )
    _write( tmp_path / 'train.toml', """\
[cell]

[[nodes]]
node_idn = "wrapper"
kind     = "cell"
toml     = "wrapper.toml"

[[variables.actors]]
target  = "wrapper.search.parameter_pick_next"
op_type = "ParameterPickTPE"
[variables.actors.parameters]
n_startup_trials = 10
""" )
    _, _, _, declarations = toml_config_tree( tmp_path / 'train.toml', node_idn='train_0' )
    resolved = resolve_actor_declarations( declarations[ 'actors' ] )
    key = 'train_0.wrapper.search.parameter_pick_next'
    assert resolved[ key ][ 'op_type' ] == 'ParameterPickTPE'
    assert resolved[ key ][ 'kwargs' ] == { 'n_startup_trials': 10 }


def test_variables_actors_root_wins_over_intermediate_default( tmp_path: Path ) -> None:
    _write( tmp_path / 'search.toml', """\
[cell]

[[nodes]]
node_idn = "parameter_pick_next"
kind     = "variable"
columns  = { a = "Int64" }
""" )
    _write( tmp_path / 'wrapper.toml', """\
[cell]

[[nodes]]
node_idn = "search"
kind     = "cell"
toml     = "search.toml"

[[variables.actors]]
target  = "search.parameter_pick_next"
op_type = "ParameterPickRandom"
""" )
    _write( tmp_path / 'train.toml', """\
[cell]

[[nodes]]
node_idn = "wrapper"
kind     = "cell"
toml     = "wrapper.toml"

[[variables.actors]]
target  = "wrapper.search.parameter_pick_next"
op_type = "ParameterPickTPE"
""" )
    _, _, _, declarations = toml_config_tree( tmp_path / 'train.toml', node_idn='train_0' )
    resolved = resolve_actor_declarations( declarations[ 'actors' ] )
    assert resolved[ 'train_0.wrapper.search.parameter_pick_next' ][ 'op_type' ] == 'ParameterPickTPE'


def test_variables_actors_requires_exactly_one_of_op_type_or_toml( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', """\
[cell]

[[nodes]]
node_idn = "picker"
kind     = "variable"
columns  = { a = "Int64" }

[[variables.actors]]
target = "picker"
""" )
    with pytest.raises( ValueError, match="exactly one of 'op_type' or 'toml'" ):
        toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )


def test_variables_actors_toml_graft_declaration_resolves_to_absolute_path( tmp_path: Path ) -> None:
    """toml_config_tree()/resolve_actor_declarations() only parse and
    resolve the declaration itself -- 'toml' is resolved to an absolute
    path (relative to the declaring file, same {key} tvars convention a
    kind='cell' [[nodes]] entry's own 'toml' gets), but nothing is spliced
    yet (that's configTree.splice_actor_grafts(), exercised end to end via
    cell_from_file in test_cell_actors.py)."""
    _write( tmp_path / 'some_sampler.toml', "[cell]\n" )
    _write( tmp_path / 'leaf.toml', """\
[cell]

[[nodes]]
node_idn = "picker"
kind     = "variable"
columns  = { a = "Int64" }

[[variables.actors]]
target = "picker"
toml   = "some_sampler.toml"
""" )
    _, _, _, declarations = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
    resolved = resolve_actor_declarations( declarations[ 'actors' ] )
    assert resolved[ 'root.picker' ][ 'kind' ] == 'cell'
    assert resolved[ 'root.picker' ][ 'toml' ] == str( ( tmp_path / 'some_sampler.toml' ).resolve() )


def test_variables_actors_toml_graft_rejects_extra_keys( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', """\
[cell]

[[nodes]]
node_idn = "picker"
kind     = "variable"
columns  = { a = "Int64" }

[[variables.actors]]
target = "picker"
toml   = "some_sampler.toml"
[variables.actors.parameters]
bogus = 1
""" )
    with pytest.raises( ValueError, match="unrecognized key" ):
        toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )


# -- kind     = "variable"


def test_variable_node_carries_columns_as_expr_text( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', """\
[cell]

[[nodes]]
node_idn = "future"
kind     = "variable"
source   = ["inp"]
target   = ["out"]
columns  = { a = "Int64", b = "Utf8" }
""" )
    tree, params, _, _ = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
    row = tree.filter( pl.col( 'node_idn' ) == 'future' ).row( 0, named = True )

    assert row[ 'kind' ]       == 'variable'
    assert row[ 'source' ]     == [ 'inp' ]
    assert row[ 'target' ]     == [ 'out' ]

    m = BDDManager()
    expr = parse_expr( row[ 'variable_columns' ] )
    assert expr.to_bdd( m ) is Union( Atom( 'a', pl.Int64 ), Atom( 'b', pl.Utf8 ) ).to_bdd( m )


def test_variable_node_defaults_to_empty_columns( tmp_path: Path ) -> None:
    _write( tmp_path / 'leaf.toml', """\
[cell]

[[nodes]]
node_idn = "future"
kind     = "variable"
""" )
    tree, _, _, _ = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
    row = tree.filter( pl.col( 'node_idn' ) == 'future' ).row( 0, named = True )
    m = BDDManager()
    from silverbrain.bdd import FALSE
    assert parse_expr( row[ 'variable_columns' ] ).to_bdd( m ) is FALSE


def test_variable_node_dict_form_infers_via_sentinel( tmp_path: Path ) -> None:
    """A dict value of "?" means "infer from schema.cannonical_schema" --
    same sentinel spelling exprParse.py's text grammar uses."""
    _write( tmp_path / 'leaf.toml', """\
[cell]

[[nodes]]
node_idn = "future"
kind     = "variable"
columns  = { op_idn = "?" }
""" )
    tree, _, _, _ = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
    row = tree.filter( pl.col( 'node_idn' ) == 'future' ).row( 0, named = True )

    from silverbrain.schema import cannonical_schema
    m = BDDManager()
    expr = parse_expr( row[ 'variable_columns' ] )
    assert expr.to_bdd( m ) is Atom( 'op_idn', cannonical_schema[ 'op_idn' ] ).to_bdd( m )


def test_variable_node_text_form_with_colvar( tmp_path: Path ) -> None:
    """The new ColVar-capable interface form -- the main use case: a saved
    map with a variable node whose interface references a column variable,
    to be resolved later once the node is filled in."""
    _write( tmp_path / 'leaf.toml', """\
[cell]

[[nodes]]
node_idn = "future"
kind     = "variable"
columns  = "Union(ColVar('C'), Atom('nu_col_0', Int64))"
""" )
    tree, _, _, _ = toml_config_tree( tmp_path / 'leaf.toml', node_idn='root' )
    row = tree.filter( pl.col( 'node_idn' ) == 'future' ).row( 0, named = True )

    m = BDDManager()
    expr = parse_expr( row[ 'variable_columns' ] )
    expected = Union( ColVar( 'C' ), Atom( 'nu_col_0', pl.Int64 ) )
    assert expr.to_bdd( m ) is expected.to_bdd( m )


def test_op_and_cell_nodes_have_null_variable_columns( tmp_path: Path ) -> None:
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
    assert rows[ 'an_op' ][ 'variable_columns' ] is None
    assert rows[ 'a_cell' ][ 'variable_columns' ] is None


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
toml     = "cells/child.toml"
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
