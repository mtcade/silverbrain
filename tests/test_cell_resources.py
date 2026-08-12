#
#//  test_cell_resources.py
#//  silverbrain
#//

"""
End-to-end tests for the variables.resources TOML extension point through
Cell/cell_from_file: a Cell's own declared resource manifest, a more senior
ancestor's path-addressed override (root-wins on a clashing table_idn),
the core motivating scenario (an override reaching an arbitrarily deep
descendant with NO intermediate cell forwarding anything), sibling
instantiations of the SAME template resolving independently, the
missing-resource construction-time failure, and the root
Python-resources=-kwarg-as-most-senior-override path. See configTree.py's
module docstring and cell.py's _cell_from_tree for the underlying
mechanics.

ReadOp (silverbrain.ReadWriteOps) is used as a real resources-declaring
TableOp -- its __init__ resolves table_idn against resources immediately
(resolve_table_idn), so construction-time behavior is observable without
ever firing the wiring engine (a node with no source never fires -- see
_WiringEngine._scan -- so these tests inspect cell.tableOps[node_idn]
directly rather than calling the Cell).
"""

from pathlib import Path

import polars as pl
import pytest

from silverbrain.cell import cell_from_file
from silverbrain.ReadWriteOps import ReadOp, load_resources


_OPS = { 'reader': ReadOp }


def _write( path: Path, content: str ) -> Path:
    path.write_text( content )
    return path


def test_own_resources_defaults_used_when_no_override( tmp_path: Path ) -> None:
    _write( tmp_path / 'main.toml', """\
[cell]

[[nodes]]
node_idn = "reader"
kind     = "op"
[nodes.parameters]
table_idn = "foo"

[[variables.resources]]
table_idn = "foo"
path      = "/default/foo.parquet"
""" )
    cell = cell_from_file( tmp_path / 'main.toml', _OPS, main_id='root' )
    assert cell.tableOps[ 'reader' ].resource.path == '/default/foo.parquet'


def test_variables_resources_direct_child_wins_over_its_own_default( tmp_path: Path ) -> None:
    _write( tmp_path / 'child.toml', """\
[cell]

[[nodes]]
node_idn = "reader"
kind     = "op"
[nodes.parameters]
table_idn = "foo"

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
path      = "/parent/override/foo.parquet"
""" )
    top = cell_from_file( tmp_path / 'top.toml', _OPS, main_id='top' )
    kid = top.tableOps[ 'kid' ]
    assert kid.tableOps[ 'reader' ].resource.path == '/parent/override/foo.parquet'


def test_variables_resources_reaches_multiple_levels_deep_with_no_forwarding( tmp_path: Path ) -> None:
    """The core new capability this rewrite exists for: train_0 (root) binds
    straight through wrapper.toml's own instantiation of search.toml, all
    the way down to reader's table_idn -- wrapper.toml never mentions
    resources at all, so a cell with an overridable descendant stays
    reusable by ANY enclosing parent, not just its direct one."""
    _write( tmp_path / 'search.toml', """\
[cell]

[[nodes]]
node_idn = "reader"
kind     = "op"
[nodes.parameters]
table_idn = "foo"
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

[[variables.resources]]
target    = "wrapper.search"
table_idn = "foo"
path      = "/train_0/reaches/deep.parquet"
""" )
    train = cell_from_file( tmp_path / 'train.toml', _OPS, main_id='train_0' )
    search = train.tableOps[ 'wrapper' ].tableOps[ 'search' ]
    assert search.tableOps[ 'reader' ].resource.path == '/train_0/reaches/deep.parquet'


def test_variables_resources_root_wins_over_intermediate_default( tmp_path: Path ) -> None:
    """An intermediate cell may still supply its own default for a
    descendant (so it works standalone too) -- but a more senior ancestor's
    override always wins over it, not just over the descendant's OWN
    default."""
    _write( tmp_path / 'search.toml', """\
[cell]

[[nodes]]
node_idn = "reader"
kind     = "op"
[nodes.parameters]
table_idn = "foo"
""" )
    _write( tmp_path / 'wrapper.toml', """\
[cell]

[[nodes]]
node_idn = "search"
kind     = "cell"
toml     = "search.toml"

[[variables.resources]]
target    = "search"
table_idn = "foo"
path      = "/wrapper/default.parquet"
""" )
    _write( tmp_path / 'train.toml', """\
[cell]

[[nodes]]
node_idn = "wrapper"
kind     = "cell"
toml     = "wrapper.toml"

[[variables.resources]]
target    = "wrapper.search"
table_idn = "foo"
path      = "/train_0/wins.parquet"
""" )
    train = cell_from_file( tmp_path / 'train.toml', _OPS, main_id='train_0' )
    search = train.tableOps[ 'wrapper' ].tableOps[ 'search' ]
    assert search.tableOps[ 'reader' ].resource.path == '/train_0/wins.parquet'


def test_sibling_instantiations_of_same_template_resolve_table_idn_differently( tmp_path: Path ) -> None:
    """Two sibling kind='cell' nodes both instantiate child.toml, each with
    its own variables.resources binding for the SAME table_idn name --
    proving per-instantiation binding resolves them to two different
    concrete resources, with no namespace mangling needed (abs_path
    disambiguates via each instance's own node_idn)."""
    _write( tmp_path / 'child.toml', """\
[cell]

[[nodes]]
node_idn = "reader"
kind     = "op"
[nodes.parameters]
table_idn = "shared_name"
""" )
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
    top = cell_from_file( tmp_path / 'top.toml', _OPS, main_id='top' )
    assert top.tableOps[ 'kid_a' ].tableOps[ 'reader' ].resource.path == '/a/data.parquet'
    assert top.tableOps[ 'kid_b' ].tableOps[ 'reader' ].resource.path == '/b/data.parquet'


def test_missing_resource_raises_at_construction_time( tmp_path: Path ) -> None:
    """A table_idn supplied by no declaration at all raises
    resolve_table_idn's ValueError -- at cell_from_file()/build_tableOps()
    construction time, not on first call. Some OTHER resource is declared
    (proving effective_resources isn't simply None) -- just not the one
    'reader' actually needs."""
    _write( tmp_path / 'main.toml', """\
[cell]

[[nodes]]
node_idn = "reader"
kind     = "op"
[nodes.parameters]
table_idn = "needed"

[[variables.resources]]
table_idn = "unrelated"
path      = "/x.parquet"
""" )
    with pytest.raises( ValueError, match="table_idn='needed' not found in resources" ):
        cell_from_file( tmp_path / 'main.toml', _OPS, main_id='root' )


def test_root_python_resources_kwarg_wins_over_everything_in_toml( tmp_path: Path ) -> None:
    """The root cell's Python-level resources= kwarg (e.g. duckpond's `dp`
    loading resources.json) is an even more senior override than anything
    declared in TOML for the root cell -- it wins over the root's own
    default on a clash."""
    _write( tmp_path / 'resources.json', '{"items": [{"table_idn": "foo", "path": "/from/json/foo.parquet"}]}' )
    _write( tmp_path / 'main.toml', """\
[cell]

[[nodes]]
node_idn = "reader"
kind     = "op"
[nodes.parameters]
table_idn = "foo"

[[variables.resources]]
table_idn = "foo"
path      = "/own/default/foo.parquet"
""" )
    resources = load_resources( tmp_path / 'resources.json' )
    cell = cell_from_file( tmp_path / 'main.toml', _OPS, main_id='root', resources=resources )
    assert cell.tableOps[ 'reader' ].resource.path == '/from/json/foo.parquet'
