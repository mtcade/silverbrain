#
#//  test_read_write_ops_sqlite.py
#//  silverbrain
#//
#//  Coverage for _write_sqlite/_read_sqlite (moved from silverflow's
#//  tableOps/__init__.py when read/write became core infrastructure --
#//  see ReadWriteOps.py). Regression tests for real bugs hit migrating
#//  hyperparameter_0 (silverknockoff)'s results.
#

import json

import polars as pl

from silverbrain.ReadWriteOps import _read_sqlite, _write_sqlite


def test_write_then_read_roundtrip( tmp_path ) -> None:
    db_path = str( tmp_path / 'results.db' )
    df = pl.DataFrame( { 'a': [ 1, 2 ], 'b': [ 'x', 'y' ] } )
    _write_sqlite( df, db_path=db_path, table_name='t', if_exists='append' )
    out = _read_sqlite( db_path, 't' )
    assert out.sort( 'a' ).to_dicts() == df.to_dicts()
#/def test_write_then_read_roundtrip


def test_append_accumulates_rows( tmp_path ) -> None:
    db_path = str( tmp_path / 'results.db' )
    _write_sqlite( pl.DataFrame( { 'a': [ 1 ] } ), db_path=db_path, table_name='t', if_exists='append' )
    _write_sqlite( pl.DataFrame( { 'a': [ 2 ] } ), db_path=db_path, table_name='t', if_exists='append' )
    out = _read_sqlite( db_path, 't' )
    assert sorted( out[ 'a' ].to_list() ) == [ 1, 2 ]
#/def test_append_accumulates_rows


def test_append_is_robust_to_column_order( tmp_path ) -> None:
    """
    Regression pin: _write_sqlite must INSERT by column name, not
    positionally -- an appended DataFrame whose columns are in a different
    order than the table's original CREATE TABLE must still land in the
    right columns.
    """
    db_path = str( tmp_path / 'results.db' )
    _write_sqlite(
        pl.DataFrame( { 'a': [ 1 ], 'b': [ 'first' ] } ),
        db_path=db_path, table_name='t', if_exists='append',
    )
    _write_sqlite(
        pl.DataFrame( { 'b': [ 'second' ], 'a': [ 2 ] } ),  # reversed column order
        db_path=db_path, table_name='t', if_exists='append',
    )
    out = _read_sqlite( db_path, 't' ).sort( 'a' )
    assert out[ 'a' ].to_list() == [ 1, 2 ]
    assert out[ 'b' ].to_list() == [ 'first', 'second' ]
#/def test_append_is_robust_to_column_order


def test_replace_drops_existing_rows( tmp_path ) -> None:
    db_path = str( tmp_path / 'results.db' )
    _write_sqlite( pl.DataFrame( { 'a': [ 1 ] } ), db_path=db_path, table_name='t', if_exists='append' )
    _write_sqlite( pl.DataFrame( { 'a': [ 2 ] } ), db_path=db_path, table_name='t', if_exists='replace' )
    out = _read_sqlite( db_path, 't' )
    assert out[ 'a' ].to_list() == [ 2 ]
#/def test_replace_drops_existing_rows


def test_fail_raises_when_table_exists( tmp_path ) -> None:
    db_path = str( tmp_path / 'results.db' )
    _write_sqlite( pl.DataFrame( { 'a': [ 1 ] } ), db_path=db_path, table_name='t', if_exists='append' )
    try:
        _write_sqlite( pl.DataFrame( { 'a': [ 2 ] } ), db_path=db_path, table_name='t', if_exists='fail' )
    except ValueError:
        pass
    else:
        assert False, "expected ValueError when if_exists='fail' and table already exists"
#/def test_fail_raises_when_table_exists


def test_list_column_round_trips_as_json( tmp_path ) -> None:
    """
    Regression pin: map_elements passes List-column element values as
    pl.Series, not plain Python lists, so writing a List column must
    convert each element before json.dumps() (json.dumps(pl.Series(...))
    raises TypeError). Stored as JSONB (see _write_sqlite_conn) -- read
    back via the json() SQL function, which re-serializes compactly (no
    spaces after ','), so compare parsed content rather than the exact
    string.
    """
    db_path = str( tmp_path / 'results.db' )
    df = pl.DataFrame( { 'nums': [ [ 1, 2, 3 ] ] } )
    _write_sqlite( df, db_path=db_path, table_name='t', if_exists='append' )
    out = _read_sqlite( db_path, 't' )
    assert json.loads( out[ 'nums' ][ 0 ] ) == [ 1, 2, 3 ]
#/def test_list_column_round_trips_as_json
