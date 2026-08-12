#
#//  test_read_write_ops.py
#//  silverbrain
#//

"""
Tests for silverbrain.ReadWriteOps: TableIndexEntry/resolve_table_idn,
load_resources/merge_resources, and the ReadOp/WriteOp/ReadWriteOp
common-constructor, per-kind-dispatch generic table ops.
"""

from pathlib import Path

import polars as pl
import pytest

from silverbrain.ReadWriteOps import (
    ReadOp,
    ReadWriteOp,
    TableIndexEntry,
    WriteOp,
    load_resources,
    merge_resources,
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


def test_resolve_table_idn_none_raises_valueerror_not_attributeerror() -> None:
    """resources=None (nothing ever declared/supplied at all) is the same
    failure as an empty/non-matching frame from a caller's perspective --
    must raise the same documented ValueError, not crash on None.filter()."""
    with pytest.raises( ValueError, match="table_idn='a'" ):
        resolve_table_idn( None, 'a' )


def test_load_resources_applies_template_vars( tmp_path: Path ) -> None:
    fp = tmp_path / 'resources.json'
    fp.write_text( '{"items": [{"table_idn": "a", "kind": "local", "path": "{bundle_root}/x.parquet"}]}' )
    df = load_resources( fp, template_vars = { 'bundle_root': '/bundles/train_0' } )
    row = df.row( 0, named = True )
    assert row[ 'table_idn' ] == 'a'
    assert row[ 'path' ] == '/bundles/train_0/x.parquet'


def test_load_resources_json_ptree_kind( tmp_path: Path ) -> None:
    fp = tmp_path / 'resources.json'
    fp.write_text( '{"items": [{"table_idn": "params", "kind": "json_ptree", "path": "/x.json"}]}' )
    df = load_resources( fp )
    assert df.row( 0, named = True )[ 'kind' ] == 'json_ptree'


def test_load_resources_rejects_bare_top_level_array( tmp_path: Path ) -> None:
    """The old {table_idn: entry} dict shape and a bare top-level array are
    both gone -- a resources.json must be a JSON object with an 'items' key
    (Kubernetes-List/GeoJSON-style wrapping, see module docstring)."""
    fp = tmp_path / 'resources.json'
    fp.write_text( '[{"table_idn": "a", "kind": "local", "path": "/x"}]' )
    with pytest.raises( ValueError, match="'items'" ):
        load_resources( fp )


def test_load_resources_empty_items_returns_empty_frame( tmp_path: Path ) -> None:
    fp = tmp_path / 'resources.json'
    fp.write_text( '{"items": []}' )
    df = load_resources( fp )
    assert df.is_empty()
    assert df.schema == pl.DataFrame( schema = table_schemas[ 'table_index' ] ).schema


def test_table_ref_row_missing_table_idn_raises() -> None:
    from silverbrain.ReadWriteOps import _table_ref_row
    with pytest.raises( ValueError, match="table_idn" ):
        _table_ref_row( { 'kind': 'local', 'path': '/x' } )


def test_merge_resources_both_none_is_none() -> None:
    assert merge_resources( None, None ) is None


def test_merge_resources_own_only() -> None:
    own = _resources( { 'table_idn': 'a', 'kind': 'local', 'path': '/x' } )
    result = merge_resources( own, None )
    assert result.to_dicts() == own.to_dicts()


def test_merge_resources_override_only() -> None:
    override = _resources( { 'table_idn': 'a', 'kind': 'local', 'path': '/y' } )
    result = merge_resources( None, override )
    assert result.to_dicts() == override.to_dicts()


def test_merge_resources_override_wins_on_clash() -> None:
    own      = _resources( { 'table_idn': 'a', 'kind': 'local', 'path': '/own' } )
    override = _resources( { 'table_idn': 'a', 'kind': 'local', 'path': '/override' } )
    result = merge_resources( own, override )
    assert result.shape[ 0 ] == 1
    assert result.row( 0, named = True )[ 'path' ] == '/override'


def test_merge_resources_no_clash_keeps_both_rows() -> None:
    own      = _resources( { 'table_idn': 'a', 'kind': 'local', 'path': '/a' } )
    override = _resources( { 'table_idn': 'b', 'kind': 'local', 'path': '/b' } )
    result = merge_resources( own, override )
    assert set( result[ 'table_idn' ].to_list() ) == { 'a', 'b' }


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


def test_sqlite_list_column_round_trips_as_jsonb( tmp_path: Path ) -> None:
    db_path = str( tmp_path / 'data.sqlite' )
    resources = _resources( { 'table_idn': 'a', 'kind': 'sqlite', 'db_path': db_path, 'table_name': 't' } )
    df = pl.DataFrame( { 'x': [ 1, 2 ], 'tags': [ [ 'a', 'b' ], [ 'c' ] ] } )
    WriteOp( table_idn = 'a', resources = resources, mode = 'replace' )( ( df, ) )

    import sqlite3
    conn = sqlite3.connect( db_path )
    try:
        decl_type = conn.execute( 'PRAGMA table_info("t")' ).fetchall()
        types_by_name = { row[1]: row[2] for row in decl_type }
        assert types_by_name[ 'tags' ] == 'BLOB'
        # jsonb() storage is not equal to the plain JSON text -- confirms
        # it's actually stored as binary jsonb, not TEXT-with-a-BLOB-label.
        raw = conn.execute( 'SELECT tags FROM "t" LIMIT 1' ).fetchone()[ 0 ]
        assert raw != '["a", "b"]'
    finally:
        conn.close()

    result = ReadOp( table_idn = 'a', resources = resources )( () )[ 0 ]
    # Read path unwraps jsonb() back to JSON text (via json()), same shape
    # a TEXT-stored column always came back as -- reconstructing pl.List
    # from that text is a separate, pre-existing gap either way.
    assert result[ 'tags' ].to_list() == [ '["a","b"]', '["c"]' ]


def test_sqlite_db_kind_requires_only_db_path() -> None:
    row = _table_ref_row_kind_check( { 'table_idn': 'a', 'kind': 'sqlite_db', 'db_path': '/x.db' } )
    assert row[ 'kind' ] == 'sqlite_db'
    assert row[ 'db_path' ] == '/x.db'


def test_sqlite_db_kind_rejects_read_op() -> None:
    resources = _resources( { 'table_idn': 'a', 'kind': 'sqlite_db', 'db_path': '/x.db' } )
    with pytest.raises( ValueError, match="kind='sqlite_db'" ):
        ReadOp( table_idn = 'a', resources = resources )


def _table_ref_row_kind_check( entry: dict ) -> dict:
    from silverbrain.ReadWriteOps import _table_ref_row
    return _table_ref_row( entry )
