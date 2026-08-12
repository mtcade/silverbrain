#
#//  test_cell_actors.py
#//  silverbrain
#//

"""
End-to-end tests for the variables.actors TOML extension point through
Cell/cell_from_file: binding a real op_type (+ ctor kwargs) to a
kind='variable' node's abs_path, root-wins over an intermediate cell's
own default, reaching an arbitrarily deep descendant with no intermediate
forwarding, an unbound placeholder still building into a PlaceholderOp
(today's exact behavior when nobody binds anything), whole-cell 'toml'
graft bindings (configTree.splice_actor_grafts), and the
__actor_bindings__ provenance table. See configTree.py's module docstring
and cell.py's _cell_from_tree for the underlying mechanics.
"""

from pathlib import Path

import polars as pl
import pytest

from silverbrain.cell import Cell, cell_from_file
from silverbrain.tableOps import PlaceholderOp


class _Picker:
    """A minimal class-shaped Actor (op_classes[node_idn](rng=rng, **kwargs)
    construction rule, same convention test_cell_from_file.py's _DoubleOp
    uses) -- just records the kwargs it was constructed with, so tests can
    tell WHICH op_type/kwargs actually got bound."""
    def __init__( self, rng = None, seed: int = 0 ) -> None:
        self.rng  = rng
        self.seed = seed
#/class _Picker


class _OtherPicker( _Picker ):
    pass
#/class _OtherPicker


_OPS = { '_Picker': _Picker, '_OtherPicker': _OtherPicker }


def _write( path: Path, content: str ) -> Path:
    path.write_text( content )
    return path


def test_unbound_variable_still_builds_placeholder_op( tmp_path: Path ) -> None:
    _write( tmp_path / 'main.toml', """\
[cell]

[[nodes]]
node_idn = "picker"
kind     = "variable"
columns  = { a = "Int64" }
""" )
    cell = cell_from_file( tmp_path / 'main.toml', _OPS, main_id='root' )
    assert isinstance( cell.tableOps[ 'picker' ], PlaceholderOp )
    assert cell.tables[ '__actor_bindings__' ].is_empty()


def test_variables_actors_binds_direct_variable( tmp_path: Path ) -> None:
    _write( tmp_path / 'main.toml', """\
[cell]

[[nodes]]
node_idn = "picker"
kind     = "variable"
columns  = { a = "Int64" }

[[variables.actors]]
target  = "picker"
op_type = "_Picker"
[variables.actors.parameters]
seed = 7
""" )
    cell = cell_from_file( tmp_path / 'main.toml', _OPS, main_id='root' )
    assert isinstance( cell.tableOps[ 'picker' ], _Picker )
    assert cell.tableOps[ 'picker' ].seed == 7

    bindings = cell.tables[ '__actor_bindings__' ].to_dicts()
    assert len( bindings ) == 1
    assert bindings[ 0 ][ 'node_idn' ]       == 'picker'
    assert bindings[ 0 ][ 'op_type' ]        == '_Picker'
    assert bindings[ 0 ][ 'declaring_path' ] == 'root'


def test_variables_actors_reaches_multiple_levels_deep_with_no_forwarding( tmp_path: Path ) -> None:
    """The core motivating scenario: duckpond's train_0 binds
    wrapper.search.picker straight through, with no forwarding declaration
    anywhere in search.toml or wrapper.toml."""
    _write( tmp_path / 'search.toml', """\
[cell]

[[nodes]]
node_idn = "picker"
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
target  = "wrapper.search.picker"
op_type = "_Picker"
[variables.actors.parameters]
seed = 42
""" )
    train  = cell_from_file( tmp_path / 'train.toml', _OPS, main_id='train_0' )
    search = train.tableOps[ 'wrapper' ].tableOps[ 'search' ]
    assert isinstance( search.tableOps[ 'picker' ], _Picker )
    assert search.tableOps[ 'picker' ].seed == 42
    # search.toml itself never declared anything -- its own __actor_bindings__
    # still records the row, since that's the Cell the binding actually
    # landed on (provenance still points back up at who declared it).
    bindings = search.tables[ '__actor_bindings__' ].to_dicts()
    assert bindings[ 0 ][ 'declaring_path' ] == 'train_0'


def test_variables_actors_root_wins_over_intermediate_default( tmp_path: Path ) -> None:
    _write( tmp_path / 'search.toml', """\
[cell]

[[nodes]]
node_idn = "picker"
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
target  = "search.picker"
op_type = "_OtherPicker"
""" )
    _write( tmp_path / 'train.toml', """\
[cell]

[[nodes]]
node_idn = "wrapper"
kind     = "cell"
toml     = "wrapper.toml"

[[variables.actors]]
target  = "wrapper.search.picker"
op_type = "_Picker"
[variables.actors.parameters]
seed = 1
""" )
    train  = cell_from_file( tmp_path / 'train.toml', _OPS, main_id='train_0' )
    search = train.tableOps[ 'wrapper' ].tableOps[ 'search' ]
    # wrapper.toml's own default for its child would have bound _OtherPicker
    # -- train_0's declaration, closer to the root, wins instead.
    assert type( search.tableOps[ 'picker' ] ) is _Picker


def test_variables_actors_requires_exactly_one_of_op_type_or_toml( tmp_path: Path ) -> None:
    _write( tmp_path / 'main.toml', """\
[cell]

[[nodes]]
node_idn = "picker"
kind     = "variable"
columns  = { a = "Int64" }

[[variables.actors]]
target = "picker"
""" )
    with pytest.raises( ValueError, match="exactly one of 'op_type' or 'toml'" ):
        cell_from_file( tmp_path / 'main.toml', _OPS, main_id='root' )


def test_variables_actors_sibling_instantiations_of_same_template_bind_independently( tmp_path: Path ) -> None:
    _write( tmp_path / 'search.toml', """\
[cell]

[[nodes]]
node_idn = "picker"
kind     = "variable"
columns  = { a = "Int64" }
""" )
    _write( tmp_path / 'top.toml', """\
[cell]

[[nodes]]
node_idn = "search_a"
kind     = "cell"
toml     = "search.toml"

[[nodes]]
node_idn = "search_b"
kind     = "cell"
toml     = "search.toml"

[[variables.actors]]
target  = "search_a.picker"
op_type = "_Picker"
[variables.actors.parameters]
seed = 1

[[variables.actors]]
target  = "search_b.picker"
op_type = "_OtherPicker"
[variables.actors.parameters]
seed = 2
""" )
    top = cell_from_file( tmp_path / 'top.toml', _OPS, main_id='top' )
    assert type( top.tableOps[ 'search_a' ].tableOps[ 'picker' ] ) is _Picker
    assert top.tableOps[ 'search_a' ].tableOps[ 'picker' ].seed == 1
    assert type( top.tableOps[ 'search_b' ].tableOps[ 'picker' ] ) is _OtherPicker
    assert top.tableOps[ 'search_b' ].tableOps[ 'picker' ].seed == 2


# -- 'toml' graft bindings: splice a whole Cell in place of a variable,
# instead of a single op_type.


def test_variables_actors_toml_graft_replaces_variable_with_a_real_cell( tmp_path: Path ) -> None:
    _write( tmp_path / 'sampler.toml', """\
[cell]
source = ["a"]
target = ["a"]

[[nodes]]
node_idn = "picker"
kind     = "op"
op_type  = "_Picker"
source   = ["a"]
target   = ["a"]
[nodes.parameters]
seed = 99
""" )
    _write( tmp_path / 'main.toml', """\
[cell]

[[nodes]]
node_idn = "picker"
kind     = "variable"
columns  = { a = "Int64" }

[[variables.actors]]
target = "picker"
toml   = "sampler.toml"
""" )
    cell = cell_from_file( tmp_path / 'main.toml', _OPS, main_id='root' )
    grafted = cell.tableOps[ 'picker' ]
    assert isinstance( grafted, Cell )
    assert isinstance( grafted.tableOps[ 'picker' ], _Picker )
    assert grafted.tableOps[ 'picker' ].seed == 99

    bindings = cell.tables[ '__actor_bindings__' ].to_dicts()
    assert bindings[ 0 ] == {
        'node_idn': 'picker', 'kind': 'cell', 'op_type': None, 'declaring_path': 'root',
    }


def test_variables_actors_toml_graft_executes_end_to_end( tmp_path: Path ) -> None:
    """The grafted Cell isn't just structurally swapped in -- it's a fully
    live, callable Cell, wired exactly as the variable declared."""
    _write( tmp_path / 'sampler.toml', """\
[cell]
source = ["a"]
target = ["a"]

[[nodes]]
node_idn = "picker"
kind     = "op"
op_type  = "_Picker"
source   = ["a"]
target   = ["a"]
""" )
    _write( tmp_path / 'main.toml', """\
[cell]
source = ["a"]
target = ["a"]

[[nodes]]
node_idn = "picker"
kind     = "variable"
columns  = { a = "Int64" }
source   = ["a"]
target   = ["a"]

[[variables.actors]]
target = "picker"
toml   = "sampler.toml"
""" )
    cell   = cell_from_file( tmp_path / 'main.toml', _OPS, main_id='root' )
    result = cell( ( pl.DataFrame( { 'a': [ 1, 2, 3 ] } ), ) ).result( timeout = 5 )
    assert result[ 0 ][ 'a' ].to_list() == [ 1, 2, 3 ]


def test_variables_actors_toml_graft_reaches_multiple_levels_deep_with_no_forwarding( tmp_path: Path ) -> None:
    _write( tmp_path / 'sampler.toml', """\
[cell]

[[nodes]]
node_idn = "impl"
kind     = "op"
op_type  = "_Picker"
[nodes.parameters]
seed = 7
""" )
    _write( tmp_path / 'search.toml', """\
[cell]

[[nodes]]
node_idn = "picker"
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
target = "wrapper.search.picker"
toml   = "sampler.toml"
""" )
    train  = cell_from_file( tmp_path / 'train.toml', _OPS, main_id='train_0' )
    search = train.tableOps[ 'wrapper' ].tableOps[ 'search' ]
    grafted = search.tableOps[ 'picker' ]
    assert isinstance( grafted, Cell )
    assert grafted.tableOps[ 'impl' ].seed == 7


def test_variables_actors_toml_graft_root_wins_over_op_type_default( tmp_path: Path ) -> None:
    """A graft ('toml') and an op_type binding compete on the SAME
    'kind' basis as any other variables.actors declaration -- whichever is
    closer to the root wins outright, not a per-field merge."""
    _write( tmp_path / 'sampler.toml', """\
[cell]

[[nodes]]
node_idn = "impl"
kind     = "op"
op_type  = "_Picker"
[nodes.parameters]
seed = 5
""" )
    _write( tmp_path / 'search.toml', """\
[cell]

[[nodes]]
node_idn = "picker"
kind     = "variable"
columns  = { a = "Int64" }

[[variables.actors]]
target  = "picker"
op_type = "_OtherPicker"
""" )
    _write( tmp_path / 'top.toml', """\
[cell]

[[nodes]]
node_idn = "search"
kind     = "cell"
toml     = "search.toml"

[[variables.actors]]
target = "search.picker"
toml   = "sampler.toml"
""" )
    top    = cell_from_file( tmp_path / 'top.toml', _OPS, main_id='top' )
    search = top.tableOps[ 'search' ]
    grafted = search.tableOps[ 'picker' ]
    assert isinstance( grafted, Cell )
    assert grafted.tableOps[ 'impl' ].seed == 5


def test_variables_actors_toml_graft_missing_variable_raises( tmp_path: Path ) -> None:
    """A graft targeting a path that never resolves to a kind='variable'
    node (typo'd target, or a target that's actually a plain op) fails
    loudly at construction time rather than being silently dropped."""
    _write( tmp_path / 'sampler.toml', "[cell]\n" )
    _write( tmp_path / 'main.toml', """\
[cell]

[[variables.actors]]
target = "does_not_exist"
toml   = "sampler.toml"
""" )
    with pytest.raises( ValueError, match="does not resolve to any kind='variable' node" ):
        cell_from_file( tmp_path / 'main.toml', _OPS, main_id='root' )
