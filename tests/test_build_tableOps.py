#
#//  test_build_tableOps.py
#//  silverbrain
#//

"""
Tests for build_tableOps() (cell.py): op_classes[op_idn](rng=rng, **kwargs)
construction, replacing the old ops_module.get_ops(keys, rng) hook -- a
plain class registry + this one generic instantiation rule, kwargs sourced
from op_ctor_kwargs_forest() (parameterParse.py).
"""

import numpy as np
import polars as pl
import pytest

from silverbrain.cell import build_tableOps
from silverbrain.parameterParse import ParameterParser


def _params_parser() -> ParameterParser:
    p = ParameterParser()
    p.register( 'op_idn',    pl.Utf8 )
    p.register( 'threshold', pl.Float64 )
    return p
#/def _params_parser


class _DefaultsOp:
    def __init__( self, rng = None, threshold: float = 1.0 ) -> None:
        self.rng       = rng
        self.threshold = threshold
#/class _DefaultsOp


class _RequiredKwargOp:
    def __init__( self, rng = None, *, threshold: float ) -> None:
        self.rng       = rng
        self.threshold = threshold
#/class _RequiredKwargOp


def test_build_tableOps_uses_class_defaults_with_empty_parameters() -> None:
    p        = _params_parser()
    empty    = pl.DataFrame( schema = p.schema )
    rng      = np.random.default_rng( 0 )
    ops      = build_tableOps( { 'a': _DefaultsOp }, [ 'a' ], rng, empty )
    assert ops[ 'a' ].rng is rng
    assert ops[ 'a' ].threshold == 1.0
#/def test_build_tableOps_uses_class_defaults_with_empty_parameters


def test_build_tableOps_passes_kwargs_from_parameters_forest() -> None:
    p          = _params_parser()
    tree, _    = p.to_polars( { 'op_idn': 'a', 'threshold': 0.25 } )
    rng        = np.random.default_rng( 0 )
    ops        = build_tableOps( { 'a': _DefaultsOp }, [ 'a' ], rng, tree )
    assert ops[ 'a' ].threshold == 0.25
#/def test_build_tableOps_passes_kwargs_from_parameters_forest


def test_build_tableOps_missing_required_kwarg_raises_typeerror() -> None:
    p     = _params_parser()
    empty = pl.DataFrame( schema = p.schema )
    with pytest.raises( TypeError ):
        build_tableOps( { 'a': _RequiredKwargOp }, [ 'a' ], None, empty )
#/def test_build_tableOps_missing_required_kwarg_raises_typeerror


def test_build_tableOps_required_kwarg_satisfied_by_parameters_forest() -> None:
    p       = _params_parser()
    tree, _ = p.to_polars( { 'op_idn': 'a', 'threshold': 0.75 } )
    ops     = build_tableOps( { 'a': _RequiredKwargOp }, [ 'a' ], None, tree )
    assert ops[ 'a' ].threshold == 0.75
#/def test_build_tableOps_required_kwarg_satisfied_by_parameters_forest


def test_build_tableOps_unknown_op_idn_raises_keyerror() -> None:
    p     = _params_parser()
    empty = pl.DataFrame( schema = p.schema )
    with pytest.raises( KeyError ):
        build_tableOps( {}, [ 'missing' ], None, empty )
#/def test_build_tableOps_unknown_op_idn_raises_keyerror


def test_build_tableOps_empty_op_keys_returns_empty_dict() -> None:
    p     = _params_parser()
    empty = pl.DataFrame( schema = p.schema )
    assert build_tableOps( { 'a': _DefaultsOp }, [], None, empty ) == {}
#/def test_build_tableOps_empty_op_keys_returns_empty_dict


def test_build_tableOps_op_types_resolves_class_independent_of_op_idn() -> None:
    """Two distinct op_idn (wiring identity) both build the same class via
    a shared op_type -- the decoupling that makes anonymous, auto-generated
    node_idn safe to reuse the same class across multiple nodes."""
    p     = _params_parser()
    empty = pl.DataFrame( schema = p.schema )
    ops   = build_tableOps(
        { 'defaults_op': _DefaultsOp }, [ 'node_1', 'node_2' ], None, empty,
        op_types = { 'node_1': 'defaults_op', 'node_2': 'defaults_op' },
    )
    assert isinstance( ops[ 'node_1' ], _DefaultsOp )
    assert isinstance( ops[ 'node_2' ], _DefaultsOp )
    assert ops[ 'node_1' ] is not ops[ 'node_2' ]
#/def test_build_tableOps_op_types_resolves_class_independent_of_op_idn


def test_build_tableOps_op_types_kwargs_stay_keyed_by_op_idn() -> None:
    """Even sharing an op_type, each op_idn's own ctor kwargs (from the
    parameters forest) stay independent -- op_types only redirects the
    class lookup, not the kwargs lookup."""
    from silverbrain.parameterParse import parameter_group_to_tree

    tree_1, next_idx = parameter_group_to_tree( 'node_1', { 'threshold': 0.1 } )
    tree_2, _        = parameter_group_to_tree( 'node_2', { 'threshold': 0.9 }, node_idx_start = next_idx )
    tree = pl.concat( ( tree_1, tree_2 ), how = 'diagonal_relaxed' )

    ops = build_tableOps(
        { 'defaults_op': _DefaultsOp }, [ 'node_1', 'node_2' ], None, tree,
        op_types = { 'node_1': 'defaults_op', 'node_2': 'defaults_op' },
    )
    assert ops[ 'node_1' ].threshold == 0.1
    assert ops[ 'node_2' ].threshold == 0.9
#/def test_build_tableOps_op_types_kwargs_stay_keyed_by_op_idn


def test_build_tableOps_op_types_omitted_matches_today_behavior() -> None:
    p     = _params_parser()
    empty = pl.DataFrame( schema = p.schema )
    ops   = build_tableOps( { 'a': _DefaultsOp }, [ 'a' ], None, empty )
    assert isinstance( ops[ 'a' ], _DefaultsOp )
#/def test_build_tableOps_op_types_omitted_matches_today_behavior


# -- resources: injected only into ops whose __init__ declares it


class _ResourcesAwareOp:
    def __init__( self, rng = None, resources = None ) -> None:
        self.rng       = rng
        self.resources = resources
#/class _ResourcesAwareOp


def test_build_tableOps_injects_resources_only_into_declaring_ops() -> None:
    p     = _params_parser()
    empty = pl.DataFrame( schema = p.schema )
    sentinel = pl.DataFrame( { 'table_idn': [ 'x' ] } )
    ops = build_tableOps(
        { 'aware': _ResourcesAwareOp, 'unaware': _DefaultsOp },
        [ 'aware', 'unaware' ], None, empty,
        resources = sentinel,
    )
    assert ops[ 'aware' ].resources is sentinel
    assert not hasattr( ops[ 'unaware' ], 'resources' )
#/def test_build_tableOps_injects_resources_only_into_declaring_ops


def test_build_tableOps_resources_omitted_defaults_to_none() -> None:
    p     = _params_parser()
    empty = pl.DataFrame( schema = p.schema )
    ops   = build_tableOps( { 'aware': _ResourcesAwareOp }, [ 'aware' ], None, empty )
    assert ops[ 'aware' ].resources is None
#/def test_build_tableOps_resources_omitted_defaults_to_none
