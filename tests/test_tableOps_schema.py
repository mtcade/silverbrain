#
#//  test_tableOps_schema.py
#//  silverbrain
#//

"""
Every reader/writer TableOp requires a `columns` ctor kwarg describing the
table it reads/writes -- "each op should only handle one table, so there
is a simple output schema known at init time." Declaration-only: nothing
here validates the declared columns against real data at __call__ time
(matches the existing pattern -- __table_op_schema__/__table_op_effects__
are descriptive metadata nothing in this codebase currently reads back to
enforce anything).
"""

import polars as pl
import pytest

from silverbrain.cell import Cell
from silverbrain.tableOps import (
    BackendReadOp,
    BackendWriteOp,
    ReadParquetOp,
    ReadPostgresOp,
    ReadSQLiteOp,
    WriteParquetOp,
    WritePostgresOp,
    WriteSQLiteOp,
)

_COLUMNS = { 'a': pl.Int64 }

_READERS = ( ReadParquetOp, ReadSQLiteOp, ReadPostgresOp, BackendReadOp )
_WRITERS = ( WriteParquetOp, WriteSQLiteOp, WritePostgresOp, BackendWriteOp )


@pytest.mark.parametrize( 'cls', _READERS + _WRITERS )
def test_columns_is_required( cls ) -> None:
    with pytest.raises( TypeError ):
        cls()
#/def test_columns_is_required


@pytest.mark.parametrize( 'cls', _READERS )
def test_reader_columns_flow_into_output_schema( cls ) -> None:
    op = cls( columns = _COLUMNS )
    assert op.output[ 0 ].columns == _COLUMNS
#/def test_reader_columns_flow_into_output_schema


@pytest.mark.parametrize( 'cls', _WRITERS )
def test_writer_columns_flow_into_data_input_schema( cls ) -> None:
    op = cls( columns = _COLUMNS )
    assert op.input[ 0 ].columns == _COLUMNS
#/def test_writer_columns_flow_into_data_input_schema


def test_register_tableOps_still_reaches_table_op_schema() -> None:
    c = Cell(
        main_id = 'c',
        ops = {
            'reader': ReadParquetOp( columns = _COLUMNS ),
            'writer': WriteParquetOp( columns = _COLUMNS ),
        },
    )
    schema = c.tables[ '__table_op_schema__' ]
    reader_output = schema.filter(
        ( pl.col( 'op_idn' ) == 'reader' ) & ( pl.col( 'direction' ) == 'output' )
    ).row( 0, named = True )
    assert reader_output[ 'column_names' ] == [ 'a' ]

    writer_input = schema.filter(
        ( pl.col( 'op_idn' ) == 'writer' ) & ( pl.col( 'direction' ) == 'input' ) & ( pl.col( 'index' ) == 0 )
    ).row( 0, named = True )
    assert writer_input[ 'column_names' ] == [ 'a' ]
#/def test_register_tableOps_still_reaches_table_op_schema
