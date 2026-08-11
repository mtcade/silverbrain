#
#//  test_cell_resources.py
#//  silverbrain
#//

"""
End-to-end tests for the new [resources]/[nodes.resources] TOML extension
point through Cell/cell_from_file: a Cell's own declared [resources]
defaults, a parent's per-instantiation [nodes.resources] override (caller
wins on a clashing table_idn), the core motivating scenario (two sibling
kind='cell' nodes instantiating the SAME template, each bound to a
different concrete resource for the same table_idn name), the
missing-resource construction-time failure, and the root
Python-resources=-kwarg-as-override path. See configTree.py's module
docstring and cell.py's _cell_from_tree for the underlying mechanics.

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

[[resources.items]]
table_idn = "foo"
path      = "/default/foo.parquet"
""" )
    cell = cell_from_file( tmp_path / 'main.toml', _OPS, main_id='root' )
    assert cell.tableOps[ 'reader' ].resource.path == '/default/foo.parquet'


def test_nodes_resources_override_wins_over_child_own_default( tmp_path: Path ) -> None:
    _write( tmp_path / 'child.toml', """\
[cell]

[[nodes]]
node_idn = "reader"
kind     = "op"
[nodes.parameters]
table_idn = "foo"

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
path      = "/parent/override/foo.parquet"
""" )
    top = cell_from_file( tmp_path / 'top.toml', _OPS, main_id='top' )
    kid = top.tableOps[ 'kid' ]
    assert kid.tableOps[ 'reader' ].resource.path == '/parent/override/foo.parquet'


def test_sibling_instantiations_of_same_template_resolve_table_idn_differently( tmp_path: Path ) -> None:
    """The core scenario: two sibling kind='cell' nodes both instantiate
    child.toml, each with its own [nodes.resources] override for the SAME
    table_idn name -- proving per-instantiation binding resolves them to
    two different concrete resources, with no namespace mangling needed."""
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
    top = cell_from_file( tmp_path / 'top.toml', _OPS, main_id='top' )
    assert top.tableOps[ 'kid_a' ].tableOps[ 'reader' ].resource.path == '/a/data.parquet'
    assert top.tableOps[ 'kid_b' ].tableOps[ 'reader' ].resource.path == '/b/data.parquet'


def test_missing_resource_raises_at_construction_time( tmp_path: Path ) -> None:
    """A table_idn supplied by neither this cell's own [resources] defaults
    nor any override raises resolve_table_idn's ValueError -- at
    cell_from_file()/build_tableOps() construction time, not on first
    call. Some OTHER resource is declared (proving effective_resources
    isn't simply None) -- just not the one 'reader' actually needs."""
    _write( tmp_path / 'main.toml', """\
[cell]

[[nodes]]
node_idn = "reader"
kind     = "op"
[nodes.parameters]
table_idn = "needed"

[[resources.items]]
table_idn = "unrelated"
path      = "/x.parquet"
""" )
    with pytest.raises( ValueError, match="table_idn='needed' not found in resources" ):
        cell_from_file( tmp_path / 'main.toml', _OPS, main_id='root' )


def test_root_python_resources_kwarg_wins_over_root_own_default( tmp_path: Path ) -> None:
    """The root cell's Python-level resources= kwarg (e.g. duckpond's `dp`
    loading resources.json) plays the exact same caller-override role a
    parent's [nodes.resources] plays for a nested instantiation -- it wins
    over the root cell's own declared [resources] default on a clash."""
    _write( tmp_path / 'resources.json', '{"items": [{"table_idn": "foo", "path": "/from/json/foo.parquet"}]}' )
    _write( tmp_path / 'main.toml', """\
[cell]

[[nodes]]
node_idn = "reader"
kind     = "op"
[nodes.parameters]
table_idn = "foo"

[[resources.items]]
table_idn = "foo"
path      = "/own/default/foo.parquet"
""" )
    resources = load_resources( tmp_path / 'resources.json' )
    cell = cell_from_file( tmp_path / 'main.toml', _OPS, main_id='root', resources=resources )
    assert cell.tableOps[ 'reader' ].resource.path == '/from/json/foo.parquet'
