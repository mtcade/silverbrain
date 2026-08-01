#
#//  test_tableOps_static.py
#//  silverbrain
#//

"""
Tests for the construction-time-parameterized (static-path) table ops:
ReadParquetStaticOp/WriteParquetStaticOp, ReadSQLiteStaticOp/
WriteSQLiteStaticOp. Unlike the existing dynamic-path ops (ReadParquetOp,
WriteSQLiteOp, ...), location is fixed at construction (ctor kwargs), not
resolved from a per-call input df -- these are a separate, additive family,
the dynamic-path ops are unchanged.
"""

from pathlib import Path

import polars as pl
import pytest

from silverbrain.cell import Cell
from silverbrain.tableOps import (
    ReadParquetStaticOp,
    ReadSQLiteStaticOp,
    WriteParquetStaticOp,
    WriteSQLiteStaticOp,
)

_COLUMNS = { 'a': pl.Int64 }
_TRIGGER = pl.DataFrame( { 'ignored': [ 1 ] } )


# -- Parquet


def test_write_then_read_parquet_static_roundtrip( tmp_path: Path ) -> None:
    fp = str( tmp_path / 'data.parquet' )
    data = pl.DataFrame( { 'a': [ 1, 2, 3 ] } )

    writer = WriteParquetStaticOp( columns=_COLUMNS, path=fp )
    writer( ( data, ) )

    reader = ReadParquetStaticOp( columns=_COLUMNS, path=fp )
    result = reader( ( _TRIGGER, ) )
    assert result[ 0 ][ 'a' ].to_list() == [ 1, 2, 3 ]


def test_write_parquet_static_echoes_written_data( tmp_path: Path ) -> None:
    fp = str( tmp_path / 'data.parquet' )
    data = pl.DataFrame( { 'a': [ 1, 2, 3 ] } )

    writer = WriteParquetStaticOp( columns=_COLUMNS, path=fp )
    result = writer( ( data, ) )
    assert result[ 0 ][ 'a' ].to_list() == data[ 'a' ].to_list()


def test_write_parquet_static_fail_raises_if_exists( tmp_path: Path ) -> None:
    fp = str( tmp_path / 'data.parquet' )
    data = pl.DataFrame( { 'a': [ 1 ] } )
    WriteParquetStaticOp( columns=_COLUMNS, path=fp )( ( data, ) )
    with pytest.raises( FileExistsError ):
        WriteParquetStaticOp( columns=_COLUMNS, path=fp, if_exists='fail' )( ( data, ) )


def test_write_parquet_static_append_accumulates( tmp_path: Path ) -> None:
    fp = str( tmp_path / 'data.parquet' )
    WriteParquetStaticOp( columns=_COLUMNS, path=fp, if_exists='append' )( ( pl.DataFrame( { 'a': [ 1 ] } ), ) )
    WriteParquetStaticOp( columns=_COLUMNS, path=fp, if_exists='append' )( ( pl.DataFrame( { 'a': [ 2 ] } ), ) )
    assert sorted( pl.read_parquet( fp )[ 'a' ].to_list() ) == [ 1, 2 ]


def test_write_parquet_static_replace_overwrites( tmp_path: Path ) -> None:
    fp = str( tmp_path / 'data.parquet' )
    WriteParquetStaticOp( columns=_COLUMNS, path=fp, if_exists='append' )( ( pl.DataFrame( { 'a': [ 1 ] } ), ) )
    WriteParquetStaticOp( columns=_COLUMNS, path=fp, if_exists='replace' )( ( pl.DataFrame( { 'a': [ 2 ] } ), ) )
    assert pl.read_parquet( fp )[ 'a' ].to_list() == [ 2 ]


def test_read_parquet_static_requires_columns_and_path() -> None:
    with pytest.raises( TypeError ):
        ReadParquetStaticOp()


def test_write_parquet_static_requires_columns_and_path() -> None:
    with pytest.raises( TypeError ):
        WriteParquetStaticOp()


# -- SQLite


def test_write_then_read_sqlite_static_roundtrip( tmp_path: Path ) -> None:
    db_path = str( tmp_path / 'data.db' )
    data = pl.DataFrame( { 'a': [ 1, 2, 3 ] } )

    writer = WriteSQLiteStaticOp( columns=_COLUMNS, db_path=db_path, table_name='t' )
    writer( ( data, ) )

    reader = ReadSQLiteStaticOp( columns=_COLUMNS, db_path=db_path, table_name='t' )
    result = reader( ( _TRIGGER, ) )
    assert sorted( result[ 0 ][ 'a' ].to_list() ) == [ 1, 2, 3 ]


def test_write_sqlite_static_echoes_written_data( tmp_path: Path ) -> None:
    db_path = str( tmp_path / 'data.db' )
    data = pl.DataFrame( { 'a': [ 1, 2, 3 ] } )
    writer = WriteSQLiteStaticOp( columns=_COLUMNS, db_path=db_path, table_name='t' )
    result = writer( ( data, ) )
    assert result[ 0 ][ 'a' ].to_list() == data[ 'a' ].to_list()


def test_write_sqlite_static_fail_raises_if_exists( tmp_path: Path ) -> None:
    db_path = str( tmp_path / 'data.db' )
    data = pl.DataFrame( { 'a': [ 1 ] } )
    WriteSQLiteStaticOp( columns=_COLUMNS, db_path=db_path, table_name='t' )( ( data, ) )
    with pytest.raises( ValueError ):
        WriteSQLiteStaticOp( columns=_COLUMNS, db_path=db_path, table_name='t', if_exists='fail' )( ( data, ) )


def test_write_sqlite_static_append_accumulates( tmp_path: Path ) -> None:
    db_path = str( tmp_path / 'data.db' )
    WriteSQLiteStaticOp( columns=_COLUMNS, db_path=db_path, table_name='t', if_exists='append' )( ( pl.DataFrame( { 'a': [ 1 ] } ), ) )
    WriteSQLiteStaticOp( columns=_COLUMNS, db_path=db_path, table_name='t', if_exists='append' )( ( pl.DataFrame( { 'a': [ 2 ] } ), ) )
    reader = ReadSQLiteStaticOp( columns=_COLUMNS, db_path=db_path, table_name='t' )
    assert sorted( reader( ( _TRIGGER, ) )[ 0 ][ 'a' ].to_list() ) == [ 1, 2 ]


def test_write_sqlite_static_replace_drops_existing_rows( tmp_path: Path ) -> None:
    db_path = str( tmp_path / 'data.db' )
    WriteSQLiteStaticOp( columns=_COLUMNS, db_path=db_path, table_name='t', if_exists='append' )( ( pl.DataFrame( { 'a': [ 1 ] } ), ) )
    WriteSQLiteStaticOp( columns=_COLUMNS, db_path=db_path, table_name='t', if_exists='replace' )( ( pl.DataFrame( { 'a': [ 2 ] } ), ) )
    reader = ReadSQLiteStaticOp( columns=_COLUMNS, db_path=db_path, table_name='t' )
    assert reader( ( _TRIGGER, ) )[ 0 ][ 'a' ].to_list() == [ 2 ]


def test_read_sqlite_static_requires_columns_db_path_table_name() -> None:
    with pytest.raises( TypeError ):
        ReadSQLiteStaticOp()


def test_write_sqlite_static_requires_columns_db_path_table_name() -> None:
    with pytest.raises( TypeError ):
        WriteSQLiteStaticOp()


# -- Cell/_WiringEngine integration: arity-1 trigger pattern


def test_read_parquet_static_fires_via_trigger_through_cell( tmp_path: Path ) -> None:
    fp = str( tmp_path / 'data.parquet' )
    pl.DataFrame( { 'a': [ 1, 2, 3 ] } ).write_parquet( fp )

    c = Cell(
        main_id='c',
        ops={ 'read_it': ReadParquetStaticOp( columns=_COLUMNS, path=fp ) },
        source=[ 'trigger' ], target=[ 'loaded' ],
    )
    c.register_node( 'read_it', source=[ 'trigger' ], target=[ 'loaded' ] )

    result = c( ( _TRIGGER, ) ).result( timeout=5 )
    assert result[ 0 ][ 'a' ].to_list() == [ 1, 2, 3 ]
