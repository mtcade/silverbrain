#
#//  test_tableOps_resolve_stub.py
#//  silverbrain
#//

"""
ResolveExpressionOp is a stub -- interface not yet finalized (see bdd.py
and tableOps.py docstrings). This just confirms it constructs and raises
cleanly, so it doesn't silently rot as the surrounding TableOp base class
evolves.
"""

import polars as pl
import pytest

from silverbrain.tableOps import ResolveExpressionOp


def test_resolve_expression_op_constructs_with_no_declared_schema() -> None:
    op = ResolveExpressionOp()
    assert op.input is None
    assert op.output is None


def test_resolve_expression_op_call_raises_uc() -> None:
    op = ResolveExpressionOp()
    with pytest.raises( NotImplementedError, match = 'UC' ):
        op( ( pl.DataFrame(), pl.DataFrame() ) )
