#
#//  test_web_toml.py
#//  silverbrain
#//
#//  Created by Evan Mason on 6/22/26.
#

"""
TOML-based tests for web_from_toml: alias_idn, exposed-name validation,
and subweb op_idn validation.
"""

from pathlib import Path
from types import SimpleNamespace

import pytest

from silverbrain.web import web_from_toml


# ops_module with no get_ops → cell loaded with empty ops dict (sufficient for
# structure-only tests that do not execute any process)
_OPS = SimpleNamespace()


# -- Helpers

def _write( path: Path, content: str ) -> Path:
    path.write_text( content )
    return path


def _cell_toml( op_idn: str, source: list[ str ], target: list[ str ] ) -> str:
    src = ', '.join( f'"{s}"' for s in source )
    tgt = ', '.join( f'"{t}"' for t in target )
    return f"""\
[[processes]]
op_idn = "{op_idn}"
type   = "TableProcessRef"
source = [{src}]
target = [{tgt}]
terms  = ["noop"]
"""


# -- Valid cases


def test_alias_idn_exposed_as_alias( tmp_path: Path ) -> None:
    """alias_idn replaces op_idn as the name the parent web sees."""
    _write( tmp_path / 'cell_a.toml', _cell_toml( 'proc_a', [ 'inp' ], [ 'out' ] ) )
    _write( tmp_path / 'web.toml', """\
[composite]
main_id = "w"

[[nodes]]
node_id = "n"
toml    = "cell_a.toml"

[[nodes.ops]]
op_idn    = "proc_a"
alias_idn = "public_a"
source    = ["inp"]
target    = ["out"]
""" )

    web   = web_from_toml( tmp_path / 'web.toml', _OPS )
    decls = web.declare()
    assert len( decls ) == 1
    assert decls[ 0 ].op_idn == 'public_a'


def test_no_alias_exposes_op_idn( tmp_path: Path ) -> None:
    """Without alias_idn the exposed name equals op_idn."""
    _write( tmp_path / 'cell_a.toml', _cell_toml( 'proc_a', [ 'inp' ], [ 'out' ] ) )
    _write( tmp_path / 'web.toml', """\
[composite]
main_id = "w"

[[nodes]]
node_id = "n"
toml    = "cell_a.toml"

[[nodes.ops]]
op_idn = "proc_a"
source = ["inp"]
target = ["out"]
""" )

    web   = web_from_toml( tmp_path / 'web.toml', _OPS )
    decls = web.declare()
    assert len( decls ) == 1
    assert decls[ 0 ].op_idn == 'proc_a'


def test_two_nodes_same_cell_op_idn_disambiguated_by_alias( tmp_path: Path ) -> None:
    """Two cells both expose 'transform'; alias_idn avoids the collision."""
    cell_toml = _cell_toml( 'transform', [ 'inp' ], [ 'out' ] )
    _write( tmp_path / 'cell_x.toml', cell_toml )
    _write( tmp_path / 'cell_y.toml', cell_toml )
    _write( tmp_path / 'web.toml', """\
[composite]
main_id = "w"

[[nodes]]
node_id = "nx"
toml    = "cell_x.toml"

[[nodes.ops]]
op_idn    = "transform"
alias_idn = "transform_x"
source    = ["inp"]
target    = ["out"]

[[nodes]]
node_id = "ny"
toml    = "cell_y.toml"

[[nodes.ops]]
op_idn    = "transform"
alias_idn = "transform_y"
source    = ["inp"]
target    = ["out"]
""" )

    web   = web_from_toml( tmp_path / 'web.toml', _OPS )
    decls = { d.op_idn for d in web.declare() }
    assert decls == { 'transform_x', 'transform_y' }


def test_subweb_valid_op_idn( tmp_path: Path ) -> None:
    """Parent references a child web's [[processes]] op_idn — should load fine."""
    _write( tmp_path / 'cell_a.toml', _cell_toml( 'proc_a', [ 'inp' ], [ 'out' ] ) )
    _write( tmp_path / 'child_web.toml', """\
[composite]
main_id = "child"

[[nodes]]
node_id = "n"
toml    = "cell_a.toml"

[[nodes.ops]]
op_idn = "proc_a"
source = ["inp"]
target = ["out"]

[[processes]]
op_idn = "child_proc"
type   = "TableProcessRef"
source = ["inp"]
target = ["out"]
terms  = ["proc_a"]
""" )
    _write( tmp_path / 'parent_web.toml', """\
[composite]
main_id = "parent"

[[nodes]]
node_id = "child"
toml    = "child_web.toml"

[[nodes.ops]]
op_idn = "child_proc"
source = ["inp"]
target = ["out"]
""" )

    web   = web_from_toml( tmp_path / 'parent_web.toml', _OPS )
    decls = { d.op_idn for d in web.declare() }
    assert decls == { 'child_proc' }


def test_subweb_alias_idn( tmp_path: Path ) -> None:
    """Subweb op_idn aliased in the parent — parent exposes the alias."""
    _write( tmp_path / 'cell_a.toml', _cell_toml( 'proc_a', [ 'inp' ], [ 'out' ] ) )
    _write( tmp_path / 'child_web.toml', """\
[composite]
main_id = "child"

[[nodes]]
node_id = "n"
toml    = "cell_a.toml"

[[nodes.ops]]
op_idn = "proc_a"
source = ["inp"]
target = ["out"]

[[processes]]
op_idn = "child_proc"
type   = "TableProcessRef"
source = ["inp"]
target = ["out"]
terms  = ["proc_a"]
""" )
    _write( tmp_path / 'parent_web.toml', """\
[composite]
main_id = "parent"

[[nodes]]
node_id = "child"
toml    = "child_web.toml"

[[nodes.ops]]
op_idn    = "child_proc"
alias_idn = "parent_alias"
source    = ["inp"]
target    = ["out"]
""" )

    web   = web_from_toml( tmp_path / 'parent_web.toml', _OPS )
    decls = web.declare()
    assert len( decls ) == 1
    assert decls[ 0 ].op_idn == 'parent_alias'


# -- Error cases


def test_duplicate_op_idn_no_alias( tmp_path: Path ) -> None:
    """Two nodes expose the same op_idn with no alias → ValueError."""
    cell_toml = _cell_toml( 'proc_a', [ 'inp' ], [ 'out' ] )
    _write( tmp_path / 'cell_x.toml', cell_toml )
    _write( tmp_path / 'cell_y.toml', cell_toml )
    _write( tmp_path / 'web.toml', """\
[composite]
main_id = "w"

[[nodes]]
node_id = "nx"
toml    = "cell_x.toml"

[[nodes.ops]]
op_idn = "proc_a"
source = ["inp"]
target = ["out"]

[[nodes]]
node_id = "ny"
toml    = "cell_y.toml"

[[nodes.ops]]
op_idn = "proc_a"
source = ["inp"]
target = ["out"]
""" )

    with pytest.raises( ValueError, match='Duplicate exposed op name' ):
        web_from_toml( tmp_path / 'web.toml', _OPS )


def test_alias_conflicts_with_other_op_idn( tmp_path: Path ) -> None:
    """alias_idn of one node equals the exposed op_idn of another → ValueError."""
    cell_toml_a = _cell_toml( 'proc_a', [ 'inp' ], [ 'out' ] )
    cell_toml_b = _cell_toml( 'proc_b', [ 'inp' ], [ 'out' ] )
    _write( tmp_path / 'cell_a.toml', cell_toml_a )
    _write( tmp_path / 'cell_b.toml', cell_toml_b )
    _write( tmp_path / 'web.toml', """\
[composite]
main_id = "w"

[[nodes]]
node_id = "na"
toml    = "cell_a.toml"

[[nodes.ops]]
op_idn    = "proc_a"
alias_idn = "proc_b"
source    = ["inp"]
target    = ["out"]

[[nodes]]
node_id = "nb"
toml    = "cell_b.toml"

[[nodes.ops]]
op_idn = "proc_b"
source = ["inp"]
target = ["out"]
""" )

    with pytest.raises( ValueError, match='Duplicate exposed op name' ):
        web_from_toml( tmp_path / 'web.toml', _OPS )


def test_op_idn_not_in_cell( tmp_path: Path ) -> None:
    """op_idn in [[nodes.ops]] that doesn't exist in the referenced cell → ValueError."""
    _write( tmp_path / 'cell_a.toml', _cell_toml( 'proc_a', [ 'inp' ], [ 'out' ] ) )
    _write( tmp_path / 'web.toml', """\
[composite]
main_id = "w"

[[nodes]]
node_id = "n"
toml    = "cell_a.toml"

[[nodes.ops]]
op_idn = "nonexistent"
source = ["inp"]
target = ["out"]
""" )

    with pytest.raises( ValueError, match="op_idn 'nonexistent' not found among ops declared by cell" ):
        web_from_toml( tmp_path / 'web.toml', _OPS )


def test_op_idn_not_in_subweb( tmp_path: Path ) -> None:
    """op_idn in [[nodes.ops]] for a subweb node that isn't exposed by the child web → ValueError."""
    _write( tmp_path / 'cell_a.toml', _cell_toml( 'proc_a', [ 'inp' ], [ 'out' ] ) )
    _write( tmp_path / 'child_web.toml', """\
[composite]
main_id = "child"

[[nodes]]
node_id = "n"
toml    = "cell_a.toml"

[[nodes.ops]]
op_idn = "proc_a"
source = ["inp"]
target = ["out"]

[[processes]]
op_idn = "child_proc"
type   = "TableProcessRef"
source = ["inp"]
target = ["out"]
terms  = ["proc_a"]
""" )
    _write( tmp_path / 'parent_web.toml', """\
[composite]
main_id = "parent"

[[nodes]]
node_id = "child"
toml    = "child_web.toml"

[[nodes.ops]]
op_idn = "nonexistent_proc"
source = ["inp"]
target = ["out"]
""" )

    with pytest.raises( ValueError, match="op_idn 'nonexistent_proc' not found in subweb" ):
        web_from_toml( tmp_path / 'parent_web.toml', _OPS )
