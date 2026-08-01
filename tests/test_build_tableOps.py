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
    tree, _    = p.to_polars( { 'a': { 'op_idn': 'a', 'threshold': 0.25 } } )
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
    tree, _ = p.to_polars( { 'a': { 'op_idn': 'a', 'threshold': 0.75 } } )
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
