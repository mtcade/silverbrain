#
#//  test_cell_from_file.py
#//  silverbrain
#//

"""
Tests for cell_from_file's nested kind='cell' [[nodes]] loading, delegated
to configTree.py to build the flat initialization table (replaces
Web/web_from_toml -- see nested-gathering-russell.md §1.6 and the
squishy-sleeping-snowflake plan): a Cell can nest other Cells directly via
tableOps, with no separate composite class and no alias/exposure machinery.
"""

from pathlib import Path

import polars as pl
import pytest

from silverbrain.bdd import BDDManager
from silverbrain.cell import Cell, cell_from_file
from silverbrain.tableOps import PlaceholderOp
from silverbrain.verify import verify_toml


class _DoubleOp:
    """A minimal class-shaped Actor -- exercises the op_classes[node_idn](rng=rng, **kwargs)
    construction rule build_tableOps() applies, not a hand-written get_ops lambda."""
    def __init__( self, rng = None ) -> None:
        self.rng = rng

    def __call__( self, dfs: tuple, corr_id: tuple = (), verbose: int = 0, verbose_prefix: str = '' ) -> tuple:
        return ( dfs[ 0 ].with_columns( ( pl.col( 'v' ) * 2 ).alias( 'v' ) ), )


_OPS = { 'double': _DoubleOp }


def _write( path: Path, content: str ) -> Path:
    path.write_text( content )
    return path


_CHILD_TOML = """\
[cell]
source = ["inp"]
target = ["out"]

[[nodes]]
node_idn = "double"
kind     = "op"
source   = ["inp"]
target   = ["out"]
"""


def _parent_toml( child_toml_name: str = 'child.toml' ) -> str:
    return f"""\
[cell]
source = ["inp"]
target = ["out"]

[[nodes]]
node_idn = "kid"
kind     = "cell"
toml     = "{child_toml_name}"
source   = ["inp"]
target   = ["out"]
"""


# -- Loading and execution


def test_nested_child_registered_as_actor( tmp_path: Path ) -> None:
    """A kind='cell' [[nodes]] entry becomes a nested Cell registered under `node_idn` in tableOps."""
    _write( tmp_path / 'child.toml', _CHILD_TOML )
    _write( tmp_path / 'parent.toml', _parent_toml() )

    parent = cell_from_file( tmp_path / 'parent.toml', _OPS, main_id='parent' )
    assert 'kid' in parent.tableOps
    assert isinstance( parent.tableOps[ 'kid' ], Cell )
    assert parent.tableOps[ 'kid' ].source == ( 'inp', )
    assert parent.tableOps[ 'kid' ].target == ( 'out', )


def test_nested_child_executes_through_parent_call( tmp_path: Path ) -> None:
    """Calling the parent Cell recurses into the nested child Cell's own op."""
    _write( tmp_path / 'child.toml', _CHILD_TOML )
    _write( tmp_path / 'parent.toml', _parent_toml() )

    parent = cell_from_file( tmp_path / 'parent.toml', _OPS, main_id='parent' )
    df_in  = pl.DataFrame( { 'v': [ 1, 2, 3 ] } )
    result = parent( ( df_in, ) ).result( timeout=5 )
    assert result[ 0 ][ 'v' ].to_list() == [ 2, 4, 6 ]


def test_resources_root_override_does_not_reach_nested_child_without_explicit_binding( tmp_path: Path ) -> None:
    """Cell.resources is no longer threaded unchanged down the whole tree.
    A resources= kwarg passed to cell_from_file() only ever becomes the
    ROOT cell's own caller-override (merged against the root's own
    declared [resources] TOML defaults, empty here) -- it does NOT
    automatically reach a nested kind='cell' child. Neither parent.toml
    nor child.toml declares any [resources]/[nodes.resources] block here,
    so the child's resources stay None even though the root's don't."""
    _write( tmp_path / 'child.toml', _CHILD_TOML )
    _write( tmp_path / 'parent.toml', _parent_toml() )

    resources = pl.DataFrame( { 'table_idn': [ 'x' ] } )
    parent = cell_from_file( tmp_path / 'parent.toml', _OPS, main_id='parent', resources=resources )
    assert parent.resources.to_dicts() == resources.to_dicts()
    assert parent.tableOps[ 'kid' ].resources is None


def test_resources_defaults_to_none( tmp_path: Path ) -> None:
    _write( tmp_path / 'child.toml', _CHILD_TOML )
    _write( tmp_path / 'parent.toml', _parent_toml() )

    parent = cell_from_file( tmp_path / 'parent.toml', _OPS, main_id='parent' )
    assert parent.resources is None
    assert parent.tableOps[ 'kid' ].resources is None


def test_bind_resolves_nested_child_directly( tmp_path: Path ) -> None:
    """bind() on the parent returns a Future-returning callable that runs the nested child Cell."""
    _write( tmp_path / 'child.toml', _CHILD_TOML )
    _write( tmp_path / 'parent.toml', _parent_toml() )

    parent = cell_from_file( tmp_path / 'parent.toml', _OPS, main_id='parent' )
    child  = parent.bind( 'kid' )
    df_in  = pl.DataFrame( { 'v': [ 5 ] } )
    assert child( ( df_in, ) ).result( timeout=5 )[ 0 ][ 'v' ].to_list() == [ 10 ]


def test_two_levels_of_nesting( tmp_path: Path ) -> None:
    """A grandchild loaded through a middle Cell still executes end to end."""
    _write( tmp_path / 'grandchild.toml', _CHILD_TOML )
    _write( tmp_path / 'middle.toml', """\
[cell]
source = ["inp"]
target = ["out"]

[[nodes]]
node_idn = "grandkid"
kind     = "cell"
toml     = "grandchild.toml"
source   = ["inp"]
target   = ["out"]
""" )
    _write( tmp_path / 'top.toml', _parent_toml( 'middle.toml' ) )

    top    = cell_from_file( tmp_path / 'top.toml', _OPS, main_id='top' )
    df_in  = pl.DataFrame( { 'v': [ 1 ] } )
    assert top( ( df_in, ) ).result( timeout=5 )[ 0 ][ 'v' ].to_list() == [ 2 ]


def test_parameters_forest_shared_across_nested_cells( tmp_path: Path ) -> None:
    """cell_from_file threads the same (currently empty) parameters frame
    to every level, rather than each Cell computing its own."""
    _write( tmp_path / 'child.toml', _CHILD_TOML )
    _write( tmp_path / 'parent.toml', _parent_toml() )

    parent = cell_from_file( tmp_path / 'parent.toml', _OPS, main_id='parent' )
    child  = parent.tableOps[ 'kid' ]
    assert parent.tables[ '__table_ops_parameters__' ].is_empty()
    assert child.tables[ '__table_ops_parameters__' ].is_empty()
    assert (
        parent.tables[ '__table_ops_parameters__' ].schema
        == child.tables[ '__table_ops_parameters__' ].schema
    )


def test_parameters_toml_syntax_flows_into_constructed_op( tmp_path: Path ) -> None:
    """A [[parameters]] entry's kwargs reach the actual TableOp instance,
    end to end through cell_from_file -> build_tableOps."""
    class _ScaleOp:
        def __init__( self, rng = None, factor: float = 2.0 ) -> None:
            self.rng    = rng
            self.factor = factor

        def __call__( self, dfs: tuple, corr_id: tuple = (), verbose: int = 0, verbose_prefix: str = '' ) -> tuple:
            return ( dfs[ 0 ].with_columns( ( pl.col( 'v' ) * self.factor ).alias( 'v' ) ), )

    _write( tmp_path / 'cell.toml', """\
[cell]
source = ["inp"]
target = ["out"]

[[nodes]]
node_idn = "scale"
kind     = "op"
source   = ["inp"]
target   = ["out"]

[[parameters]]
node_idn = "scale"
factor   = 10.0
""" )

    cell   = cell_from_file( tmp_path / 'cell.toml', { 'scale': _ScaleOp }, main_id='root' )
    assert cell.tableOps[ 'scale' ].factor == 10.0
    df_in  = pl.DataFrame( { 'v': [ 1, 2 ] } )
    result = cell( ( df_in, ) ).result( timeout=5 )
    assert result[ 0 ][ 'v' ].to_list() == [ 10, 20 ]


def test_missing_child_toml_raises( tmp_path: Path ) -> None:
    _write( tmp_path / 'parent.toml', _parent_toml( 'nonexistent.toml' ) )
    with pytest.raises( FileNotFoundError ):
        cell_from_file( tmp_path / 'parent.toml', _OPS, main_id='parent' )


def test_unsupported_format_raises( tmp_path: Path ) -> None:
    _write( tmp_path / 'parent.yaml', "cell: {}\n" )
    with pytest.raises( ValueError, match="Unsupported config file format" ):
        cell_from_file( tmp_path / 'parent.yaml', _OPS, main_id='parent' )


def test_unknown_node_kind_raises( tmp_path: Path ) -> None:
    _write( tmp_path / 'cell.toml', """\
[cell]

[[nodes]]
node_idn = "mystery"
kind     = "bogus"
""" )
    with pytest.raises( ValueError, match="unknown kind" ):
        cell_from_file( tmp_path / 'cell.toml', _OPS, main_id='root' )


# -- kind="placeholder"


def test_placeholder_node_builds_placeholder_op( tmp_path: Path ) -> None:
    _write( tmp_path / 'cell.toml', """\
[cell]
source = ["inp"]
target = ["out"]

[[nodes]]
node_idn = "future"
kind     = "placeholder"
source   = ["inp"]
target   = ["out"]
columns  = { a = "Int64" }
""" )

    cell = cell_from_file( tmp_path / 'cell.toml', {}, main_id='root' )
    op   = cell.tableOps[ 'future' ]
    assert isinstance( op, PlaceholderOp )
    m = BDDManager()
    # input carries an extra='open' ColVar (see PlaceholderOp/tableOps.py),
    # so it's not fully grounded -- check the declared atom's dtype directly.
    op.input[ 0 ].expr.to_bdd( m )
    assert m.dtype( 'a' ) == pl.Int64
    assert m.resolve_to_typed_columns( op.output[ 0 ].expr.to_bdd( m ) ) == { 'a': pl.Int64 }


def test_placeholder_node_with_colvar_interface( tmp_path: Path ) -> None:
    """The main use case: a saved map with a placeholder whose interface
    references a ColVar (exprParse.py's text grammar), not just literal
    columns -- once this placeholder is later filled in with a real op/
    Cell, bdd.resolve_signatures can infer more about the whole cell's
    columns from whatever binds that ColVar."""
    _write( tmp_path / 'cell.toml', """\
[cell]
source = ["inp"]
target = ["out"]

[[nodes]]
node_idn = "future"
kind     = "placeholder"
source   = ["inp"]
target   = ["out"]
columns  = "Union(ColVar('C'), Atom('nu_col_0', Int64))"
""" )

    cell = cell_from_file( tmp_path / 'cell.toml', {}, main_id='root' )
    op   = cell.tableOps[ 'future' ]
    assert isinstance( op, PlaceholderOp )

    from silverbrain.bdd import Atom, ColVar, Union
    m = BDDManager()
    expected = Union( ColVar( 'C' ), Atom( 'nu_col_0', pl.Int64 ) )
    assert op.expr.to_bdd( m ) is expected.to_bdd( m )

    # genuinely unresolved -- resolve_to_columns must refuse to guess.
    with pytest.raises( ValueError, match = 'C' ):
        m.resolve_to_columns( op.expr.to_bdd( m ) )

    # but bdd.resolve_signatures (the machinery this feature is *for*) can
    # resolve it once a binding for C shows up.
    from silverbrain.bdd import resolve_signatures
    resolved = resolve_signatures( m, { 'sig': op.expr }, { 'C': Atom( 'nu_col_1', pl.Utf8 ) } )
    assert m.resolve_to_columns( resolved[ 'sig' ].to_bdd( m ) ) == frozenset( { 'nu_col_0', 'nu_col_1' } )


def test_placeholder_node_raises_if_actually_called( tmp_path: Path ) -> None:
    _write( tmp_path / 'cell.toml', """\
[cell]
source = ["inp"]
target = ["out"]

[[nodes]]
node_idn = "future"
kind     = "placeholder"
source   = ["inp"]
target   = ["out"]
columns  = { a = "Int64" }
""" )

    cell   = cell_from_file( tmp_path / 'cell.toml', {}, main_id='root' )
    future = cell( ( pl.DataFrame( { 'a': [ 1 ] } ), ) )
    with pytest.raises( NotImplementedError ):
        future.result( timeout=5 )


def test_placeholder_can_be_swapped_post_init( tmp_path: Path ) -> None:
    """Nothing about tableOps[node_idn] pins a node to its build-time kind --
    replacing the dict entry is enough for future dispatches to pick it up
    (Cell._WiringEngine looks up self.cell.tableOps[node_idn] live)."""
    _write( tmp_path / 'cell.toml', """\
[cell]
source = ["inp"]
target = ["out"]

[[nodes]]
node_idn = "future"
kind     = "placeholder"
source   = ["inp"]
target   = ["out"]
columns  = { a = "Int64" }
""" )

    cell = cell_from_file( tmp_path / 'cell.toml', {}, main_id='root' )
    cell.tableOps[ 'future' ] = _DoubleOp()
    result = cell( ( pl.DataFrame( { 'v': [ 3 ] } ), ) ).result( timeout=5 )
    assert result[ 0 ][ 'v' ].to_list() == [ 6 ]


# -- verify_toml


def test_verify_valid_nested_toml( tmp_path: Path ) -> None:
    _write( tmp_path / 'child.toml', _CHILD_TOML )
    _write( tmp_path / 'parent.toml', _parent_toml() )
    assert verify_toml( tmp_path / 'parent.toml' ) == []


def test_verify_missing_child_toml( tmp_path: Path ) -> None:
    _write( tmp_path / 'parent.toml', _parent_toml( 'nonexistent.toml' ) )
    issues = verify_toml( tmp_path / 'parent.toml' )
    assert any( 'toml path not found' in str( i ) for i in issues )


def test_verify_duplicate_node_idn( tmp_path: Path ) -> None:
    _write( tmp_path / 'child.toml', _CHILD_TOML )
    _write( tmp_path / 'parent.toml', """\
[cell]

[[nodes]]
node_idn = "kid"
kind     = "cell"
toml     = "child.toml"

[[nodes]]
node_idn = "kid"
kind     = "cell"
toml     = "child.toml"
""" )
    issues = verify_toml( tmp_path / 'parent.toml' )
    assert any( 'Duplicate node_idn' in str( i ) for i in issues )


def test_verify_unknown_kind( tmp_path: Path ) -> None:
    _write( tmp_path / 'cell.toml', """\
[cell]

[[nodes]]
node_idn = "mystery"
kind     = "bogus"
""" )
    issues = verify_toml( tmp_path / 'cell.toml' )
    assert any( 'unknown kind' in str( i ) for i in issues )


def test_verify_placeholder_missing_columns( tmp_path: Path ) -> None:
    _write( tmp_path / 'cell.toml', """\
[cell]

[[nodes]]
node_idn = "future"
kind     = "placeholder"
""" )
    issues = verify_toml( tmp_path / 'cell.toml' )
    assert any( "missing 'columns'" in str( i ) for i in issues )
