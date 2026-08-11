#
#//  test_read_write_ops.py
#//  silverbrain
#//

"""
Tests for silverbrain.ReadWriteOps: TableIndexEntry/resolve_table_idn,
load_manifest, and the ReadOp/WriteOp/ReadWriteOp common-constructor,
per-kind-dispatch generic table ops.
"""

from pathlib import Path

import polars as pl
import pytest

from silverbrain.ReadWriteOps import (
    ReadOp,
    ReadWriteOp,
    TableIndexEntry,
    WriteOp,
    load_manifest,
    resolve_table_idn,
    table_schemas,
)


def _resources( *rows: dict ) -> pl.DataFrame:
    base = { 'path': None, 'db_path': None, 'table_name': None, 'connection_string': None, 'schema_name': None }
    return pl.DataFrame( [ base | row for row in rows ], schema = table_schemas[ 'table_index' ] )


def test_table_index_entry_from_row() -> None:
    row = { 'table_idn': 'a', 'kind': 'local', 'path': '/x', 'db_path': None, 'table_name': None, 'connection_string': None, 'schema_name': None }
    entry = TableIndexEntry.from_row( row )
    assert entry == TableIndexEntry( table_idn='a', kind='local', path='/x' )


def test_resolve_table_idn_found() -> None:
    resources = _resources( { 'table_idn': 'a', 'kind': 'local', 'path': '/x' } )
    assert resolve_table_idn( resources, 'a' ) == TableIndexEntry( table_idn='a', kind='local', path='/x' )


def test_resolve_table_idn_missing_raises() -> None:
    resources = _resources( { 'table_idn': 'a', 'kind': 'local', 'path': '/x' } )
    with pytest.raises( ValueError, match="table_idn='missing'" ):
        resolve_table_idn( resources, 'missing' )


def test_load_manifest_applies_template_vars( tmp_path: Path ) -> None:
    fp = tmp_path / 'manifest.json'
    fp.write_text( '{"a": {"kind": "local", "path": "{bundle_root}/x.parquet"}}' )
    df = load_manifest( fp, template_vars = { 'bundle_root': '/bundles/train_0' } )
    row = df.row( 0, named = True )
    assert row[ 'table_idn' ] == 'a'
    assert row[ 'path' ] == '/bundles/train_0/x.parquet'


def test_load_manifest_json_ptree_kind( tmp_path: Path ) -> None:
    fp = tmp_path / 'manifest.json'
    fp.write_text( '{"params": {"kind": "json_ptree", "path": "/x.json"}}' )
    df = load_manifest( fp )
    assert df.row( 0, named = True )[ 'kind' ] == 'json_ptree'


def test_read_op_rejects_json_ptree_kind() -> None:
    resources = _resources( { 'table_idn': 'a', 'kind': 'json_ptree', 'path': '/x.json' } )
    with pytest.raises( ValueError, match="kind='json_ptree'" ):
        ReadOp( table_idn = 'a', resources = resources )


def test_read_op_rejects_unknown_table_idn() -> None:
    resources = _resources( { 'table_idn': 'a', 'kind': 'local', 'path': '/x' } )
    with pytest.raises( ValueError, match="table_idn='missing'" ):
        ReadOp( table_idn = 'missing', resources = resources )


def test_write_op_rejects_invalid_mode() -> None:
    resources = _resources( { 'table_idn': 'a', 'kind': 'local', 'path': '/x' } )
    with pytest.raises( ValueError, match="mode" ):
        WriteOp( table_idn = 'a', resources = resources, mode = 'bogus' )


def test_local_parquet_round_trip( tmp_path: Path ) -> None:
    fp = str( tmp_path / 'data.parquet' )
    resources = _resources( { 'table_idn': 'a', 'kind': 'local', 'path': fp } )
    df_in = pl.DataFrame( { 'x': [ 1, 2, 3 ] } )

    WriteOp( table_idn = 'a', resources = resources, mode = 'replace' )( ( df_in, ) )
    result = ReadOp( table_idn = 'a', resources = resources )( () )
    assert result[ 0 ].to_dicts() == df_in.to_dicts()


def test_local_parquet_append_mode( tmp_path: Path ) -> None:
    fp = str( tmp_path / 'data.parquet' )
    resources = _resources( { 'table_idn': 'a', 'kind': 'local', 'path': fp } )
    writer = WriteOp( table_idn = 'a', resources = resources, mode = 'append' )
    writer( ( pl.DataFrame( { 'x': [ 1 ] } ), ) )
    writer( ( pl.DataFrame( { 'x': [ 2 ] } ), ) )
    result = ReadOp( table_idn = 'a', resources = resources )( () )
    assert result[ 0 ][ 'x' ].to_list() == [ 1, 2 ]


def test_read_write_op_writes_then_reads_back_full_state( tmp_path: Path ) -> None:
    fp = str( tmp_path / 'data.parquet' )
    resources = _resources( { 'table_idn': 'a', 'kind': 'local', 'path': fp } )
    rw = ReadWriteOp( table_idn = 'a', resources = resources, mode = 'append' )
    first  = rw( ( pl.DataFrame( { 'x': [ 1 ] } ), ) )
    second = rw( ( pl.DataFrame( { 'x': [ 2 ] } ), ) )
    assert first[ 0 ][ 'x' ].to_list()  == [ 1 ]
    assert second[ 0 ][ 'x' ].to_list() == [ 1, 2 ]


def test_sqlite_round_trip_with_persistent_connection( tmp_path: Path ) -> None:
    db_path = str( tmp_path / 'data.sqlite' )
    resources = _resources( { 'table_idn': 'a', 'kind': 'sqlite', 'db_path': db_path, 'table_name': 't' } )
    writer = WriteOp( table_idn = 'a', resources = resources, mode = 'replace' )
    writer( ( pl.DataFrame( { 'x': [ 10, 20 ] } ), ) )
    # Same op instance's connection reused across calls -- write again (replace).
    writer( ( pl.DataFrame( { 'x': [ 30 ] } ), ) )
    reader = ReadOp( table_idn = 'a', resources = resources )
    result = reader( () )
    assert result[ 0 ][ 'x' ].to_list() == [ 30 ]


def test_postgres_kind_raises_not_implemented() -> None:
    resources = _resources( { 'table_idn': 'a', 'kind': 'postgres', 'connection_string': 'x', 'table_name': 't' } )
    with pytest.raises( NotImplementedError ):
        ReadOp( table_idn = 'a', resources = resources )
