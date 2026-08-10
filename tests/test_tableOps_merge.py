#
#//  test_tableOps_merge.py
#//  silverbrain
#//

"""
Tests for MergeOp -- see its docstring in tableOps.py. Meant to be
registered with join='any' (see test_cell_nodes.py for that wired through a
live Cell); these tests exercise MergeOp directly, without a Cell.
"""

import polars as pl
import pytest

from silverbrain.tableOps import MergeOp


def test_requires_at_least_two_inputs() -> None:
    with pytest.raises( ValueError ):
        MergeOp( n = 1 )
    with pytest.raises( ValueError ):
        MergeOp( n = 0 )


def test_forwards_the_only_non_none_input() -> None:
    op = MergeOp( n = 2 )
    df = pl.DataFrame( { 'a': [ 1 ] } )
    assert op( ( df, None ) ) == ( df, )
    assert op( ( None, df ) ) == ( df, )


def test_last_non_none_wins_when_more_than_one_present() -> None:
    op = MergeOp( n = 3 )
    first  = pl.DataFrame( { 'a': [ 1 ] } )
    second = pl.DataFrame( { 'a': [ 2 ] } )
    assert op( ( first, None, second ) ) == ( second, )
    assert op( ( first, second, None ) ) == ( second, )


def test_all_none_raises() -> None:
    op = MergeOp( n = 2 )
    with pytest.raises( ValueError ):
        op( ( None, None ) )


def test_wrong_arity_raises() -> None:
    op = MergeOp( n = 3 )
    with pytest.raises( ValueError ):
        op( ( pl.DataFrame( { 'a': [ 1 ] } ), None ) )
