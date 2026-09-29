#
#//  test_deployment.py
#//  silverbrain
#//

"""
Tests for the deployment mechanism: a caller-supplied {abs_path, strategy,
options_json} DataFrame deciding WHERE a leaf TableOp is constructed,
entirely orthogonal to a bundle's own TOML (never written into any TOML,
supplied directly to cell_from_file alongside the already-traversed tree --
see configTree.resolve_deployment()'s own docstring and cell.py's
build_tableOps/_construct_op).

Covers three layers: configTree.resolve_deployment() itself (longest
abs_path-prefix inheritance, defaulting to 'local'), build_tableOps()'s new
deployment/abs_paths params in isolation, and cell_from_file() end-to-end,
including a real Ray actor construction+dispatch (not just a strategy-tag
check) to prove the existing hasattr(actor, 'remote') dispatch path in
cell.py's _call_actor actually receives a working handle.
"""

from pathlib import Path

import polars as pl
import pytest

from silverbrain.cell import build_tableOps, cell_from_file
from silverbrain.configTree import resolve_deployment


def _write( path: Path, content: str ) -> Path:
    path.write_text( content )
    return path


def _deployment_frame( rows: list[ dict ] ) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema = { 'abs_path': pl.Utf8, 'strategy': pl.Utf8, 'options_json': pl.Utf8 },
    )
#/def _deployment_frame


# -- resolve_deployment(): prefix-inheritance resolution in isolation


def test_resolve_deployment_none_defaults_every_node_to_local() -> None:
    tree = pl.DataFrame( { 'abs_path': [ 'root', 'root.a', 'root.a.b' ] } )
    resolved = resolve_deployment( None, tree )
    assert resolved == {
        'root':     { 'strategy': 'local', 'options': {} },
        'root.a':   { 'strategy': 'local', 'options': {} },
        'root.a.b': { 'strategy': 'local', 'options': {} },
    }
#/def test_resolve_deployment_none_defaults_every_node_to_local


def test_resolve_deployment_exact_match() -> None:
    tree = pl.DataFrame( { 'abs_path': [ 'root', 'root.a' ] } )
    deployment = _deployment_frame( [
        { 'abs_path': 'root.a', 'strategy': 'ray', 'options_json': '{"num_cpus": 2}' },
    ] )
    resolved = resolve_deployment( deployment, tree )
    assert resolved[ 'root' ]   == { 'strategy': 'local', 'options': {} }
    assert resolved[ 'root.a' ] == { 'strategy': 'ray', 'options': { 'num_cpus': 2 } }
#/def test_resolve_deployment_exact_match


def test_resolve_deployment_inherits_from_ancestor_prefix() -> None:
    """A row declared for a nested Cell's own root abs_path ('root.wrapper')
    reaches an arbitrarily deep descendant ('root.wrapper.search.picker')
    with no intermediate row -- same 'declare once, flows down' shape
    variables.resources/variables.actors already give TOML declarations."""
    tree = pl.DataFrame( { 'abs_path': [
        'root', 'root.wrapper', 'root.wrapper.search', 'root.wrapper.search.picker',
    ] } )
    deployment = _deployment_frame( [
        { 'abs_path': 'root.wrapper', 'strategy': 'ray', 'options_json': None },
    ] )
    resolved = resolve_deployment( deployment, tree )
    assert resolved[ 'root' ][ 'strategy' ] == 'local'
    for abs_path in ( 'root.wrapper', 'root.wrapper.search', 'root.wrapper.search.picker' ):
        assert resolved[ abs_path ] == { 'strategy': 'ray', 'options': {} }
#/def test_resolve_deployment_inherits_from_ancestor_prefix


def test_resolve_deployment_most_specific_row_wins_over_ancestor() -> None:
    tree = pl.DataFrame( { 'abs_path': [ 'root', 'root.a', 'root.a.b' ] } )
    deployment = _deployment_frame( [
        { 'abs_path': 'root.a',   'strategy': 'ray',   'options_json': None },
        { 'abs_path': 'root.a.b', 'strategy': 'local',  'options_json': None },
    ] )
    resolved = resolve_deployment( deployment, tree )
    assert resolved[ 'root.a' ][ 'strategy' ]   == 'ray'
    assert resolved[ 'root.a.b' ][ 'strategy' ] == 'local'
#/def test_resolve_deployment_most_specific_row_wins_over_ancestor


def test_resolve_deployment_empty_frame_same_as_none() -> None:
    tree = pl.DataFrame( { 'abs_path': [ 'root' ] } )
    empty = _deployment_frame( [] )
    assert resolve_deployment( empty, tree ) == resolve_deployment( None, tree )
#/def test_resolve_deployment_empty_frame_same_as_none


# -- build_tableOps(): deployment/abs_paths params in isolation


class _DefaultsOp:
    def __init__( self, rng = None, threshold: float = 1.0 ) -> None:
        self.rng       = rng
        self.threshold = threshold

    def get_threshold( self ) -> float:
        return self.threshold
#/class _DefaultsOp


def _empty_params() -> pl.DataFrame:
    from silverbrain.parameterParse import ParameterParser
    return pl.DataFrame( schema = ParameterParser().schema )
#/def _empty_params


def test_build_tableOps_omitted_deployment_matches_today_behavior() -> None:
    ops = build_tableOps( { 'a': _DefaultsOp }, [ 'a' ], None, _empty_params() )
    assert isinstance( ops[ 'a' ], _DefaultsOp )
#/def test_build_tableOps_omitted_deployment_matches_today_behavior


def test_build_tableOps_local_strategy_builds_instance_directly() -> None:
    deployment = { 'root.a': { 'strategy': 'local', 'options': {} } }
    ops = build_tableOps(
        { 'a': _DefaultsOp }, [ 'a' ], None, _empty_params(),
        deployment = deployment, abs_paths = { 'a': 'root.a' },
    )
    assert isinstance( ops[ 'a' ], _DefaultsOp )
#/def test_build_tableOps_local_strategy_builds_instance_directly


def test_build_tableOps_unrecognized_strategy_raises() -> None:
    deployment = { 'root.a': { 'strategy': 'bogus', 'options': {} } }
    with pytest.raises( ValueError, match="unknown deployment strategy" ):
        build_tableOps(
            { 'a': _DefaultsOp }, [ 'a' ], None, _empty_params(),
            deployment = deployment, abs_paths = { 'a': 'root.a' },
        )
#/def test_build_tableOps_unrecognized_strategy_raises


def test_build_tableOps_ray_strategy_returns_a_remote_actor_handle() -> None:
    """No ray.init() needed just to construct the actor handle -- ray lazily
    auto-starts a local-mode instance on first .remote() the same way
    cell.py's own _call_actor lazy-imports ray, so this stays a fast,
    single-process test."""
    ray = pytest.importorskip( 'ray' )
    deployment = { 'root.a': { 'strategy': 'ray', 'options': {} } }
    ops = build_tableOps(
        { 'a': _DefaultsOp }, [ 'a' ], None, _empty_params(),
        deployment = deployment, abs_paths = { 'a': 'root.a' },
    )
    assert hasattr( ops[ 'a' ], '_ray_actor_id' )
    try:
        threshold = ray.get( ops[ 'a' ].get_threshold.remote() )
        assert threshold == 1.0
    finally:
        ray.shutdown()
#/def test_build_tableOps_ray_strategy_returns_a_remote_actor_handle


# -- cell_from_file(): end-to-end, including a real Ray dispatch round trip


class _DoubleOp:
    """Same minimal class-shaped Actor convention test_cell_from_file.py's
    _DoubleOp uses."""
    def __init__( self, rng = None ) -> None:
        self.rng = rng

    def __call__( self, dfs: tuple, corr_id: tuple = (), verbose: int = 0, verbose_prefix: str = '' ) -> tuple:
        return ( dfs[ 0 ].with_columns( ( pl.col( 'v' ) * 2 ).alias( 'v' ) ), )
#/class _DoubleOp


_OPS = { 'double': _DoubleOp }

_MAIN_TOML = """\
[cell]
source = ["inp"]
target = ["out"]

[[nodes]]
node_idn = "double"
kind     = "op"
source   = ["inp"]
target   = ["out"]
"""


def test_cell_from_file_omitted_deployment_matches_today_behavior( tmp_path: Path ) -> None:
    _write( tmp_path / 'main.toml', _MAIN_TOML )
    cell = cell_from_file( tmp_path / 'main.toml', _OPS, main_id = 'root' )
    assert isinstance( cell.tableOps[ 'double' ], _DoubleOp )
    result = cell( ( pl.DataFrame( { 'v': [ 1, 2, 3 ] } ), ) ).result( timeout = 5 )
    assert result[ 0 ][ 'v' ].to_list() == [ 2, 4, 6 ]
#/def test_cell_from_file_omitted_deployment_matches_today_behavior


def test_cell_from_file_local_deployment_row_matches_today_behavior( tmp_path: Path ) -> None:
    _write( tmp_path / 'main.toml', _MAIN_TOML )
    deployment = _deployment_frame( [
        { 'abs_path': 'root.double', 'strategy': 'local', 'options_json': None },
    ] )
    cell = cell_from_file( tmp_path / 'main.toml', _OPS, main_id = 'root', deployment = deployment )
    assert isinstance( cell.tableOps[ 'double' ], _DoubleOp )
#/def test_cell_from_file_local_deployment_row_matches_today_behavior


def test_cell_from_file_ray_deployment_executes_end_to_end( tmp_path: Path ) -> None:
    """The core scenario: a Cell built with a Ray-placed leaf op still
    executes correctly through Cell.__call__ -- cell.py's existing
    _call_actor dispatch (hasattr(actor, 'remote')) needed no change at all
    to make this work, proving build_tableOps' construction-time decision
    is the whole story."""
    ray = pytest.importorskip( 'ray' )
    _write( tmp_path / 'main.toml', _MAIN_TOML )
    deployment = _deployment_frame( [
        { 'abs_path': 'root.double', 'strategy': 'ray', 'options_json': None },
    ] )
    try:
        cell = cell_from_file( tmp_path / 'main.toml', _OPS, main_id = 'root', deployment = deployment )
        assert hasattr( cell.tableOps[ 'double' ], '_ray_actor_id' )
        result = cell( ( pl.DataFrame( { 'v': [ 1, 2, 3 ] } ), ) ).result( timeout = 10 )
        assert result[ 0 ][ 'v' ].to_list() == [ 2, 4, 6 ]
    finally:
        ray.shutdown()
#/def test_cell_from_file_ray_deployment_executes_end_to_end


def test_cell_from_file_deployment_inherits_into_nested_cell( tmp_path: Path ) -> None:
    """A deployment row targeting a nested Cell's own abs_path reaches its
    descendant op with no forwarding declaration anywhere in the child's
    own TOML -- mirrors variables.actors' own no-forwarding scenario, but
    for placement instead of implementation."""
    _write( tmp_path / 'child.toml', """\
[cell]
source = ["inp"]
target = ["out"]

[[nodes]]
node_idn = "double"
kind     = "op"
source   = ["inp"]
target   = ["out"]
""" )
    _write( tmp_path / 'main.toml', """\
[cell]
source = ["inp"]
target = ["out"]

[[nodes]]
node_idn = "wrapper"
kind     = "cell"
toml     = "child.toml"
source   = ["inp"]
target   = ["out"]
""" )
    ray = pytest.importorskip( 'ray' )
    deployment = _deployment_frame( [
        { 'abs_path': 'root.wrapper', 'strategy': 'ray', 'options_json': None },
    ] )
    try:
        cell = cell_from_file( tmp_path / 'main.toml', _OPS, main_id = 'root', deployment = deployment )
        assert hasattr( cell.tableOps[ 'wrapper' ].tableOps[ 'double' ], '_ray_actor_id' )
    finally:
        ray.shutdown()
#/def test_cell_from_file_deployment_inherits_into_nested_cell
