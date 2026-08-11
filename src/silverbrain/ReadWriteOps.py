#
#//  ReadWriteOps.py
#//  silverbrain
#//

"""
Resource resolution + generic read/write/read-write TableOps, keyed by
table_idn against a `resources` table_index DataFrame (see cell.py's
Cell.resources -- held as a top-level attribute of every Cell and threaded
to every child Cell/TableOp construction, the same way the
__table_ops_parameters__ forest already is).

A `resources` manifest is a table_index DataFrame (schema.py's
table_schemas['table_index']): one row per named resource, keyed by
table_idn, naming where it lives (`kind` selects which of the remaining
mostly-null columns matter -- see schema.py's own comment on those
columns). `load_manifest()` parses a manifest.json file (the same
{table_idn: entry} shape _table_ref_row already understands) into this
DataFrame.

ReadOp/WriteOp/ReadWriteOp are the public, generic TableOps: each takes
`table_idn`+`resources` at construction, resolves the one specific
resource it names via `resolve_table_idn` (raising immediately if
table_idn is missing, or if its kind isn't a raw-table kind they support),
and dispatches internally to a small per-kind implementation
(_LocalParquetImpl/_SQLiteImpl/_PostgresImpl) -- one common constructor
per direction, not one hand-written class per backend x per direction.
kind='json_ptree' (a parameter-tree-shaped JSON file) is deliberately
rejected here -- it's silverflow.tableOps.ReadParametersOp's own concern,
not a raw table read/write.
"""

from . import bdd
from .schema import InputSchema, OutputSchema, table_schemas
from .tableOps import TableOp
from .utilities import apply_template_vars

import polars as pl
import numpy as np

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Self
from uuid import uuid4
import json
import sqlite3


# -- TableIndexEntry: a resolved table_index row, Python-native types

@dataclass( frozen = True )
class TableIndexEntry:
    """One resolved table_index row -- see schema.py's cannonical_schema
    comment on 'db_path'/'table_name'/'connection_string'/'schema_name'
    for what each column means per 'kind'. Python-native types (not
    pl.DataType) since this is what a TableOp actually works with at
    construction/call time, not a DataFrame schema declaration."""
    table_idn:          str
    kind:                str
    path:                str | None = None
    db_path:             str | None = None
    table_name:          str | None = None
    connection_string:   str | None = None
    schema_name:         str | None = None

    @classmethod
    def from_row( cls, row: dict[ str, Any ] ) -> Self:
        """row: a table_index DataFrame's .row(0, named=True) -- already
        matches this dataclass's field names/types 1:1."""
        return cls( **{ k: row[ k ] for k in (
            'table_idn', 'kind', 'path', 'db_path',
            'table_name', 'connection_string', 'schema_name',
        ) } )
    #/def from_row
#/class TableIndexEntry


def resolve_table_idn( resources: pl.DataFrame, table_idn: str ) -> TableIndexEntry:
    """Look up table_idn in resources (a table_index DataFrame), raising
    if it isn't present -- construction-time resolution, so a missing
    table_idn fails immediately at Cell-build time, not on first use."""
    rows = resources.filter( pl.col( 'table_idn' ) == table_idn )
    if rows.is_empty():
        raise ValueError( f"table_idn={table_idn!r} not found in resources manifest" )
    return TableIndexEntry.from_row( rows.row( 0, named = True ) )
#/def resolve_table_idn


# -- table_index parsing (manifest.json / any {table_idn: entry} JSON file)

# Which table_index columns are required for each 'kind' -- checked eagerly
# in _table_ref_row so a malformed table_index source file fails at the
# read, not later/silently when some consumer reaches for a null it didn't
# expect. 'json_ptree' (a parameter-tree-shaped JSON file -- silverflow's
# ReadParametersOp only, never ReadOp/WriteOp/ReadWriteOp here) needs just
# 'path', same as 'local'.
_TABLE_INDEX_REQUIRED_COLUMNS: dict[ str, tuple[ str, ... ] ] = {
    'local':      ( 'path', ),
    'sqlite':     ( 'db_path', 'table_name' ),
    'postgres':   ( 'connection_string', 'table_name' ),
    'json_ptree': ( 'path', ),
}

def _table_ref_row( table_idn: str, entry: str | dict[ str, Any ] ) -> dict[ str, Any ]:
    """A bare string entry is shorthand for {'kind': 'local', 'path':
    entry}. Otherwise entry must be a dict; 'kind' defaults to 'local' if
    omitted (the common case, and the only kind a bare path string could
    mean)."""
    if isinstance( entry, str ):
        entry = { 'kind': 'local', 'path': entry }
    if not isinstance( entry, dict ):
        raise TypeError(
            f"table_index entry for {table_idn!r} must be a string (local "
            f"path shorthand) or a dict, got {entry!r}"
        )
    kind = entry.get( 'kind', 'local' )
    if kind not in _TABLE_INDEX_REQUIRED_COLUMNS:
        raise ValueError(
            f"table_index entry for {table_idn!r} has unrecognized kind={kind!r}, "
            f"expected one of {tuple( _TABLE_INDEX_REQUIRED_COLUMNS )}"
        )
    missing = tuple(
        col for col in _TABLE_INDEX_REQUIRED_COLUMNS[ kind ]
        if entry.get( col ) is None
    )
    if missing:
        raise ValueError(
            f"table_index entry for {table_idn!r} (kind={kind!r}) is missing "
            f"required column(s) {missing}"
        )
    return { 'table_idn': table_idn, 'kind': kind } | {
        col: entry.get( col )
        for col in table_schemas[ 'table_index' ] if col not in ( 'table_idn', 'kind' )
    }
#/def _table_ref_row

def load_manifest( fp: str | Path, template_vars: dict[ str, str ] | None = None ) -> pl.DataFrame:
    """Parse a manifest.json-shaped file ({table_idn: entry_or_path_string})
    into a table_index DataFrame. template_vars (e.g. {'bundle_root': ...})
    is applied to each entry's 'path'/'db_path' first, same convention
    configTree.py already uses for TOML-declared paths -- lets a manifest
    reference {bundle_root}/... without knowing the bundle's actual
    location."""
    tvars = template_vars or {}
    with open( fp, 'r' ) as f:
        data: dict[ str, Any ] = json.load( f )
    def _substitute( entry: str | dict[ str, Any ] ) -> str | dict[ str, Any ]:
        if isinstance( entry, str ):
            return apply_template_vars( entry, tvars )
        return {
            k: ( apply_template_vars( v, tvars ) if k in ( 'path', 'db_path' ) and isinstance( v, str ) else v )
            for k, v in entry.items()
        }
    rows = [ _table_ref_row( table_idn, _substitute( entry ) ) for table_idn, entry in data.items() ]
    if rows:
        return pl.DataFrame( rows, schema = table_schemas[ 'table_index' ] )
    return pl.DataFrame( schema = table_schemas[ 'table_index' ] )
#/def load_manifest


# -- Backend primitives (parquet/sqlite), persistent-connection variants

def _pl_dtype_to_sqlite_type( dtype: pl.DataType ) -> str:
    if dtype == pl.Boolean:
        return 'INTEGER'
    if dtype.is_integer():
        return 'INTEGER'
    if dtype.is_float():
        return 'REAL'
    return 'TEXT'
#/def _pl_dtype_to_sqlite_type

def _json_dumps_element( v: Any ) -> str | None:
    """map_elements passes List/Array element values as pl.Series, not
    plain Python lists/dicts -- normalize before handing to json.dumps."""
    if v is None:
        return None
    if isinstance( v, pl.Series ):
        v = v.to_list()
    return json.dumps( v )
#/def _json_dumps_element

def _prepare_df_for_sqlite( df: pl.DataFrame ) -> pl.DataFrame:
    """Serialize List/Struct/Array columns to JSON strings for SQLite storage."""
    exprs = [
        pl.col( col ).map_elements( _json_dumps_element, return_dtype = pl.String ).alias( col )
        for col, dtype in df.schema.items()
        if isinstance( dtype, ( pl.List, pl.Array, pl.Struct ) )
    ]
    return df.with_columns( exprs ) if exprs else df
#/def _prepare_df_for_sqlite

def _write_parquet( df: pl.DataFrame, fp: str, if_exists: str ) -> None:
    if if_exists == 'fail' and Path( fp ).is_file():
        raise FileExistsError( f"Parquet file already exists: {fp}" )
    if if_exists == 'append' and Path( fp ).is_file():
        df = pl.concat( [ pl.read_parquet( fp ), df ] )
    df.write_parquet( fp )
#/def _write_parquet

def _write_sqlite_conn( conn: sqlite3.Connection, table_name: str, if_exists: str, df: pl.DataFrame ) -> None:
    """Same logic as a one-shot _write_sqlite, operating on an
    already-open connection (see _SQLiteImpl -- opened once at
    construction, reused across every call)."""
    df = _prepare_df_for_sqlite( df )
    cursor = conn.cursor()
    exists = cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", ( table_name, )
    ).fetchone() is not None
    if exists:
        if if_exists == 'fail':
            raise ValueError( f"Table '{table_name}' already exists" )
        elif if_exists == 'replace':
            cursor.execute( f'DROP TABLE "{table_name}"' )
            exists = False
    if not exists:
        col_defs = ', '.join(
            f'"{col}" {_pl_dtype_to_sqlite_type( dtype )}'
            for col, dtype in df.schema.items()
        )
        cursor.execute( f'CREATE TABLE "{table_name}" ({col_defs})' )
    col_list     = ', '.join( f'"{col}"' for col in df.columns )
    placeholders = ', '.join( '?' for _ in df.columns )
    cursor.executemany(
        f'INSERT INTO "{table_name}" ({col_list}) VALUES ({placeholders})',
        df.rows(),
    )
    conn.commit()
#/def _write_sqlite_conn

def _read_sqlite_conn( conn: sqlite3.Connection, table_name: str ) -> pl.DataFrame:
    cursor = conn.cursor()
    cursor.execute( f'SELECT * FROM "{table_name}"' )
    rows = cursor.fetchall()
    col_names = [ desc[0] for desc in cursor.description ]
    if not rows:
        return pl.DataFrame( { name: [] for name in col_names } )
    return pl.DataFrame( rows, schema = col_names, orient = 'row', infer_schema_length = None )
#/def _read_sqlite_conn

def _write_sqlite( df: pl.DataFrame, db_path: str, table_name: str, if_exists: str ) -> None:
    """One-shot (open, write, close) -- see _write_sqlite_conn for the
    persistent-connection variant ReadOp/WriteOp/ReadWriteOp use."""
    conn = sqlite3.connect( db_path )
    try:
        _write_sqlite_conn( conn, table_name, if_exists, df )
    finally:
        conn.close()
#/def _write_sqlite

def _read_sqlite( db_path: str, table_name: str ) -> pl.DataFrame:
    conn = sqlite3.connect( db_path )
    try:
        return _read_sqlite_conn( conn, table_name )
    finally:
        conn.close()
#/def _read_sqlite


# -- Per-kind implementations (internal -- composed into ReadOp/WriteOp/
# ReadWriteOp below, not TableOps themselves)

class _LocalParquetImpl:
    def __init__( self: Self, resource: TableIndexEntry ) -> None:
        self.path = resource.path
    def read( self: Self ) -> pl.DataFrame:
        return pl.read_parquet( self.path )
    def write( self: Self, df: pl.DataFrame, mode: str ) -> None:
        _write_parquet( df, self.path, mode )
#/class _LocalParquetImpl

class _SQLiteImpl:
    def __init__( self: Self, resource: TableIndexEntry ) -> None:
        self.table_name = resource.table_name
        self._conn      = sqlite3.connect( resource.db_path )
    def read( self: Self ) -> pl.DataFrame:
        return _read_sqlite_conn( self._conn, self.table_name )
    def write( self: Self, df: pl.DataFrame, mode: str ) -> None:
        _write_sqlite_conn( self._conn, self.table_name, mode, df )
#/class _SQLiteImpl

class _PostgresImpl:
    def __init__( self: Self, resource: TableIndexEntry ) -> None:
        raise NotImplementedError( 'UC: postgres backend not yet implemented' )
#/class _PostgresImpl

_IMPLS: dict[ str, type ] = { 'local': _LocalParquetImpl, 'sqlite': _SQLiteImpl, 'postgres': _PostgresImpl }


def _open_output_schema() -> OutputSchema:
    """Content is unknown ahead of time (it comes from whichever resource
    table_idn resolves to) -- a fresh anonymous ColVar Union, same 'open'
    shape PlaceholderOp/MergeOp use for their own unconstrained slots."""
    return OutputSchema( expr = bdd.Union( bdd.ColVar( f'_rwops_out_{uuid4().hex}' ) ) )
#/def _open_output_schema


# -- Public ops

class ReadOp( TableOp ):
    """table_idn+resources resolved at construction (raises if missing, or
    if its kind isn't one of 'local'/'sqlite'/'postgres' -- 'json_ptree' is
    silverflow.tableOps.ReadParametersOp's concern, not this). Trivial
    input (ignored -- _WiringEngine never dispatches a node with an empty
    declared source list); output is whatever the resolved resource holds."""
    def __init__(
        self: Self,
        table_idn: str,
        resources: pl.DataFrame,
        rng: np.random.Generator | None = None,
        ) -> None:
        self.resource = resolve_table_idn( resources, table_idn )
        if self.resource.kind not in _IMPLS:
            raise ValueError(
                f"ReadOp: table_idn={table_idn!r} has kind={self.resource.kind!r}, "
                f"expected one of {tuple( _IMPLS )}"
            )
        self._impl = _IMPLS[ self.resource.kind ]( self.resource )
        super().__init__(
            rng    = rng,
            input  = [ InputSchema.from_columns( {}, extra = 'open' ) ],
            output = [ _open_output_schema() ],
        )
    #/def __init__

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame, ... ],
        corr_id: tuple[ str, ... ] = (),
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame ]:
        if verbose > 0:
            print( f"{verbose_prefix}: Reading {self.resource.kind}: {self.resource.table_idn}" )
        return ( self._impl.read(), )
    #/def __call__
#/class ReadOp

class WriteOp( TableOp ):
    """table_idn+resources resolved at construction, same kind
    restriction as ReadOp. mode: 'append' or 'replace' -- required (no
    default; a write op that doesn't say which it means is a bug waiting
    to happen). Input: the table to write; output: the same table,
    echoed (a "trivial result" -- pairs with cell.py's "_" discard target
    convention for callers that don't want it wired anywhere)."""
    def __init__(
        self: Self,
        table_idn: str,
        resources: pl.DataFrame,
        mode: Literal[ 'append', 'replace' ],
        rng: np.random.Generator | None = None,
        ) -> None:
        if mode not in ( 'append', 'replace' ):
            raise ValueError( f"WriteOp: mode must be 'append' or 'replace', got {mode!r}" )
        self.resource = resolve_table_idn( resources, table_idn )
        if self.resource.kind not in _IMPLS:
            raise ValueError(
                f"WriteOp: table_idn={table_idn!r} has kind={self.resource.kind!r}, "
                f"expected one of {tuple( _IMPLS )}"
            )
        self._impl = _IMPLS[ self.resource.kind ]( self.resource )
        self.mode  = mode
        super().__init__(
            rng    = rng,
            input  = [ InputSchema.from_columns( {}, extra = 'open' ) ],
            output = [ _open_output_schema() ],
        )
    #/def __init__

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame, ... ],
        corr_id: tuple[ str, ... ] = (),
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame ]:
        data = dfs[ 0 ]
        if verbose > 0:
            print( f"{verbose_prefix}: Writing {self.resource.kind} (mode={self.mode}): {self.resource.table_idn}" )
        self._impl.write( data, self.mode )
        return ( data, )
    #/def __call__
#/class WriteOp

class ReadWriteOp( TableOp ):
    """Combines WriteOp and ReadOp: writes dfs[0] per `mode`, then reads
    back and returns the resource's current full content -- "take the
    referenced table for both input and output.\""""
    def __init__(
        self: Self,
        table_idn: str,
        resources: pl.DataFrame,
        mode: Literal[ 'append', 'replace' ],
        rng: np.random.Generator | None = None,
        ) -> None:
        if mode not in ( 'append', 'replace' ):
            raise ValueError( f"ReadWriteOp: mode must be 'append' or 'replace', got {mode!r}" )
        self.resource = resolve_table_idn( resources, table_idn )
        if self.resource.kind not in _IMPLS:
            raise ValueError(
                f"ReadWriteOp: table_idn={table_idn!r} has kind={self.resource.kind!r}, "
                f"expected one of {tuple( _IMPLS )}"
            )
        self._impl = _IMPLS[ self.resource.kind ]( self.resource )
        self.mode  = mode
        super().__init__(
            rng    = rng,
            input  = [ InputSchema.from_columns( {}, extra = 'open' ) ],
            output = [ _open_output_schema() ],
        )
    #/def __init__

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame, ... ],
        corr_id: tuple[ str, ... ] = (),
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame ]:
        if verbose > 0:
            print( f"{verbose_prefix}: Read/write {self.resource.kind} (mode={self.mode}): {self.resource.table_idn}" )
        self._impl.write( dfs[ 0 ], self.mode )
        return ( self._impl.read(), )
    #/def __call__
#/class ReadWriteOp
