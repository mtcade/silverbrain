#
#//  test_tableOps_empty.py
#//  silverbrain
#//

"""
Tests for EmptyTableOp: ignores its input, always returns a 0-row
DataFrame with the schema named by table_schema (a schema.table_schemas
key) -- for bootstrapping a self-referencing loop's first iteration (see
cell.py's discard_on_null / README.md's ### Loops section).
"""

import polars as pl
import pytest

from silverbrain.tableOps import EmptyTableOp
from silverbrain.schema import table_schemas


def test_returns_zero_row_frame_matching_named_schema() -> None:
    op = EmptyTableOp( table_schema = 'parameter_tree' )
    result = op( ( pl.DataFrame( { 'x': [ 1, 2, 3 ] } ), ) )
    assert len( result ) == 1
    assert result[ 0 ].is_empty()
    assert result[ 0 ].schema == pl.Schema( table_schemas[ 'parameter_tree' ] )


def test_ignores_its_input() -> None:
    op = EmptyTableOp( table_schema = 'parameter_tree' )
    a = op( ( pl.DataFrame( { 'x': [ 1 ] } ), ) )
    b = op( ( pl.DataFrame( { 'y': [ 'anything' ], 'z': [ True ] } ), ) )
    assert a[ 0 ].to_dicts() == b[ 0 ].to_dicts() == []
    assert a[ 0 ].schema == b[ 0 ].schema


def test_unknown_table_schema_raises() -> None:
    with pytest.raises( ValueError, match="table_schema='nope'" ):
        EmptyTableOp( table_schema = 'nope' )
