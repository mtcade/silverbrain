#
#//  tableOps.py
#//  silverbrain
#//
#//  Created by Evan Mason on 3/23/26.
#//

from . import types
from .schema import table_schemas

import polars as pl
import time

from dataclasses import dataclass
from typing import Self, Sequence, Type

# -- Serialization helper

def _make_process_row( **kwargs ) -> pl.DataFrame:
    """
    Build a single-row DataFrame with the '__table_processes__' schema.
    Any column not supplied in kwargs is set to null.
    """
    defaults: dict = {
        'op_idn':     None,
        'type':       None,
        'source':     None,
        'target':     None,
        'term_idns':  None,
        'condition':  None,
        'count':      None,
        'ifs':        None,
        'thens':      None,
        'node_idn':   None,
    }
    defaults.update( kwargs )
    return pl.DataFrame(
        {
            k: [ v ]
            for k, v in defaults.items()
        },
        schema = table_schemas[
            '__table_processes__'
        ],
    )
#/def _make_process_row

# -- DAG traversal helper

def collect_process_deps( rows: dict, op_idn: str ) -> set[ str ]:
    """
    Return op_idn and all transitively referenced process op_idns.
    Stops at TableProcessRef/TableCheckRef (their terms are tableOp keys, not processes).
    """
    result: set[ str ] = set()
    stack  = [ op_idn ]
    while stack:
        oid = stack.pop()
        if oid in result or oid not in rows:
            continue
        result.add( oid )
        row = rows[ oid ]
        if row[ 'type' ] not in ( 'TableProcessRef', 'TableCheckRef' ):
            for t in ( row.get( 'term_idns' ) or [] ):
                stack.append( t )
            for t in ( row.get( 'ifs' ) or [] ):
                stack.append( t )
            for t in ( row.get( 'thens' ) or [] ):
                stack.append( t )
            if cond := row.get( 'condition' ):
                stack.append( cond )
    return result
#/def collect_process_deps

# -- DataFrame interpreter helpers

def _apply_process(
    rows: dict,
    op_idn: str,
    tables: dict[ str, 'pl.DataFrame' ],
    tableOps: 'types.TableProcessDict',
    verbose: int = 0,
    verbose_prefix: str = '',
    ) -> 'tuple[ pl.DataFrame, ... ]':
    row = rows[ op_idn ]
    t   = row[ 'type' ]

    if verbose > 0:
        print( verbose_prefix + "{} {}".format( row[ 'op_idn' ], row[ 'source' ] ) )
    #

    if t == 'TableProcessRef':
        result = tableOps[ row[ 'term_idns' ][ 0 ] ](
            dfs            = tuple( tables[ tid ] for tid in row[ 'source' ] ),
            verbose        = verbose,
            verbose_prefix = verbose_prefix,
        )
    #
    elif t == 'TableProcessSequence':
        local = dict( tables )
        for term_op_idn in row[ 'term_idns' ]:
            r = _apply_process( rows, term_op_idn, local, tableOps, verbose, verbose_prefix + "  " )
            local |= dict( zip( rows[ term_op_idn ][ 'target' ], r ) )
        #/for term_op_idn
        result = tuple( local[ tid ] for tid in row[ 'target' ] )
    #
    elif t == 'TableProcessWhile':
        local       = dict( tables )
        proc_op_idn = row[ 'term_idns' ][ 0 ]
        max_iter    = row[ 'count' ] if row[ 'count' ] is not None else 100
        iterations  = 0
        while _apply_check( rows, row[ 'condition' ], local, tableOps, verbose, verbose_prefix + "  " ):
            if iterations >= max_iter:
                raise Exception( "reached max_iter={}".format( max_iter ) )
            r = _apply_process( rows, proc_op_idn, local, tableOps, verbose, verbose_prefix + "  " )
            local |= dict( zip( rows[ proc_op_idn ][ 'target' ], r ) )
            iterations += 1
        #/while
        result = tuple( local[ tid ] for tid in row[ 'target' ] )
    #
    elif t == 'TableProcessCount':
        local       = dict( tables )
        proc_op_idn = row[ 'term_idns' ][ 0 ]
        for _ in range( row[ 'count' ] ):
            r = _apply_process( rows, proc_op_idn, local, tableOps, verbose, verbose_prefix + "  " )
            local |= dict( zip( rows[ proc_op_idn ][ 'target' ], r ) )
        #/for
        result = tuple( local[ tid ] for tid in row[ 'target' ] )
    #
    elif t == 'TableProcessSleep':
        local       = dict( tables )
        proc_op_idn = row[ 'term_idns' ][ 0 ]
        r = _apply_process( rows, proc_op_idn, local, tableOps, verbose, verbose_prefix + "  " )
        local |= dict( zip( rows[ proc_op_idn ][ 'target' ], r ) )
        time.sleep( float( row[ 'condition' ] ) )
        result = tuple( local[ tid ] for tid in row[ 'target' ] )
    #
    elif t == 'TableProcessBranch':
        local = dict( tables )
        for if_op_idn, then_op_idn in zip( row[ 'ifs' ], row[ 'thens' ] ):
            if _apply_check( rows, if_op_idn, local, tableOps, verbose, verbose_prefix + "  " ):
                r = _apply_process( rows, then_op_idn, local, tableOps, verbose, verbose_prefix + "  " )
                local |= dict( zip( rows[ then_op_idn ][ 'target' ], r ) )
                break
            #/if
        else:
            if row[ 'term_idns' ]:
                otherwise_op_idn = row[ 'term_idns' ][ 0 ]
                r = _apply_process( rows, otherwise_op_idn, local, tableOps, verbose, verbose_prefix + "  " )
                local |= dict( zip( rows[ otherwise_op_idn ][ 'target' ], r ) )
        #/for
        result = tuple( local.get( tid ) for tid in row[ 'target' ] )
    #
    else:
        raise ValueError( "Unknown type: {}".format( t ) )
    #

    if verbose > 0:
        print( verbose_prefix + "/{} {}".format( row[ 'op_idn' ], row[ 'target' ] ) )
    #

    return result
#/def _apply_process

def _apply_check(
    rows: dict,
    op_idn: str,
    tables: dict[ str, 'pl.DataFrame' ],
    tableOps: 'types.TableProcessDict',
    verbose: int = 0,
    verbose_prefix: str = '',
    ) -> bool:
    row    = rows[ op_idn ]

    if verbose > 0:
        print( verbose_prefix + "{} {}".format( row[ 'op_idn' ], row[ 'source' ] ) )
    #

    result = tableOps[ row[ 'term_idns' ][ 0 ] ](
        dfs            = tuple( tables[ tid ] for tid in row[ 'source' ] ),
        verbose        = verbose,
        verbose_prefix = verbose_prefix,
    )

    if verbose > 0:
        print( verbose_prefix + "/{} {}".format( row[ 'op_idn' ], result ) )
    #

    return result
#/def _apply_check

def run_from_df(
    df: pl.DataFrame,
    root_op_idn: str,
    dfs: tuple[ pl.DataFrame, ... ],
    tableOps: types.TableProcessDict,
    verbose: int = 0,
    verbose_prefix: str = '',
    ) -> tuple[ pl.DataFrame, ... ]:
    rows    = { r[ 'op_idn' ]: r for r in df.to_dicts() }
    top_row = rows[ root_op_idn ]
    tables  = dict( zip( top_row[ 'source' ], dfs ) )
    return _apply_process(
        rows,
        root_op_idn,
        tables,
        tableOps,
        verbose,
        verbose_prefix,
    )
#/def run_from_df

# -- Alias; lightweight data holder for register_process()

@dataclass
class Alias():
    op_idn:  str
    aliases: str            # op_idn of the process being aliased
    source:  Sequence[ str ]
    target:  Sequence[ str ]

    def as_polars( self: Self ) -> pl.DataFrame:
        return _make_process_row(
            op_idn    = self.op_idn,
            type      = 'TableProcessSequence',
            source    = list( self.source ),
            target    = list( self.target ),
            term_idns =[ self.aliases ],
        )
    #/def as_polars
#/class Alias


# -- Process Ref; Atomic types.TaggedTableProcess

@dataclass
class TableProcessRef():
    source: Sequence[ str ]
    target: Sequence[ str ]
    terms: Sequence[ str ]
    op_idn: str
    """
        Holds a reference to an atomic types.TableProcess. All together acts as an atomic types.TaggedTableProcess.
        terms is a single-element list: [op_key_string].
    """

    def as_polars( self: Self ) -> pl.DataFrame:
        return _make_process_row(
            op_idn    = self.op_idn,
            type      = 'TableProcessRef',
            source    = list( self.source ),
            target    = list( self.target ),
            term_idns =list( self.terms ),
        )
    #/def as_polars

    def alias( self, new_op_idn: str ) -> Alias:
        return Alias(
            op_idn=new_op_idn,
            aliases=self.op_idn,
            source=self.source,
            target=self.target,
        )
    #/def alias
#/class TableProcessRef

@dataclass
class TableCheckRef():
    """
        Used mostly for `TableProcessWhile`. `TableProcessBranch`.
        terms is a single-element list: [op_key_string].
    """
    source: Sequence[ str ]
    terms:  Sequence[ str ]
    op_idn: str

    def as_polars( self: Self ) -> pl.DataFrame:
        return _make_process_row(
            op_idn    = self.op_idn,
            type      = 'TableCheckRef',
            source    = list( self.source ),
            target    = [],
            term_idns =list( self.terms ),
        )
    #/def as_polars
#/class TableCheckRef

def always_true( op_idn: str = '_always_true' ) -> TableCheckRef:
    """Return a TableCheckRef whose check always returns True."""
    return TableCheckRef(
        source = [],
        terms  = [ '_always_true' ],
        op_idn = op_idn,
    )
#/def always_true

def always_false( op_idn: str = '_always_false' ) -> TableCheckRef:
    """Return a TableCheckRef whose check always returns False."""
    return TableCheckRef(
        source = [],
        terms  = [ '_always_false' ],
        op_idn = op_idn,
    )
#/def always_false

@dataclass
class TableProcessSequence():
    source: Sequence[ str ]
    target: Sequence[ str ]
    terms: Sequence[ str ]
    op_idn: str

    def as_polars( self: Self ) -> pl.DataFrame:
        return _make_process_row(
            op_idn    = self.op_idn,
            type      = 'TableProcessSequence',
            source    = list( self.source ),
            target    = list( self.target ),
            term_idns =list( self.terms ),
        )
    #/def as_polars

    def alias( self, new_op_idn: str ) -> Alias:
        return Alias( op_idn=new_op_idn, aliases=self.op_idn, source=self.source, target=self.target )
    #/def alias
#/class TableProcessSequence

@dataclass
class TableProcessWhile():
    source: Sequence[ str ]
    target: Sequence[ str ]
    condition: str
    terms: Sequence[ str ]
    op_idn: str
    maxIter: int = 2**31 - 1

    def as_polars( self: Self ) -> pl.DataFrame:
        return _make_process_row(
            op_idn    = self.op_idn,
            type      = 'TableProcessWhile',
            source    = list( self.source ),
            target    = list( self.target ),
            condition = self.condition,
            term_idns =list( self.terms ),
            count     = self.maxIter,
        )
    #/def as_polars
#/class TableProcessWhile

@dataclass
class TableProcessSleep():
    """
    Runs its single ``terms`` item, then sleeps for ``float(condition)`` seconds.
    ``condition`` is a string-encoded float (e.g. ``"5.0"``).
    """
    source:    Sequence[ str ]
    target:    Sequence[ str ]
    condition: str
    terms:     Sequence[ str ]
    op_idn:    str

    def as_polars( self: Self ) -> pl.DataFrame:
        return _make_process_row(
            op_idn    = self.op_idn,
            type      = 'TableProcessSleep',
            source    = list( self.source ),
            target    = list( self.target ),
            condition = self.condition,
            term_idns = list( self.terms ),
        )
    #/def as_polars
#/class TableProcessSleep

@dataclass
class TableProcessCount():
    source: Sequence[ str ]
    target: Sequence[ str ]
    count: int
    terms: Sequence[ str ]
    op_idn: str

    def as_polars( self: Self ) -> pl.DataFrame:
        return _make_process_row(
            op_idn    = self.op_idn,
            type      = 'TableProcessCount',
            source    = list( self.source ),
            target    = list( self.target ),
            term_idns =list( self.terms ),
            count     = self.count,
        )
    #/def as_polars
#/class TableProcessCount
    
@dataclass
class TableProcessBranch():
    op_idn: str
    source: Sequence[ str ]
    target: Sequence[ str ]
    ifs: Sequence[ str ]
    thens: Sequence[ str ]
    otherwise: str | None = None

    def as_polars( self: Self ) -> pl.DataFrame:
        return _make_process_row(
            op_idn = self.op_idn,
            type   = 'TableProcessBranch',
            source = list( self.source ),
            target = list( self.target ),
            ifs    = list( self.ifs ),
            thens  = list( self.thens ),
            term_idns =[ self.otherwise ] if self.otherwise is not None else [],
        )
    #/def as_polars
#/class TableProcessBranch

# -- Backend Storage Process Factories
#
# These wrap BackendReadOp / BackendWriteOp in a TableProcessRef so callers don't
# need to list all five backend source tables by hand.  Register the corresponding
# op instances under the same key strings in your tableOps dict:
#
#   tableOps = {
#       'BackendReadOp':        BackendReadOp(),
#       'BackendWriteOp':       BackendWriteOp(),
#       'IsParquetBackendOp':   IsParquetBackendOp(),
#       'IsSQLiteBackendOp':    IsSQLiteBackendOp(),
#       'IsPostgresBackendOp':  IsPostgresBackendOp(),
#   }

def backend_read_process(
    op_idn: str,
    source_backend_ref: str,
    target_data: str,
    source_backend_index:   str = '__backend_index__',
    source_parquet_backend: str = '__parquet_backend__',
    source_sqlite_backend:  str = '__sqlite_backend__',
    source_postgres_backend: str = '__postgres_backend__',
    ) -> TableProcessRef:
    """
    TableProcessRef that reads from the backend identified by `source_backend_ref`.
    Dispatches to the correct backend via BackendReadOp.
    """
    return TableProcessRef(
        op_idn  = op_idn,
        source  = [
            source_backend_ref,
            source_backend_index,
            source_parquet_backend,
            source_sqlite_backend,
            source_postgres_backend,
        ],
        target  = [ target_data ],
        terms   = [ 'BackendReadOp' ],
    )
#/def backend_read_process

def backend_write_process(
    op_idn: str,
    source_data: str,
    source_backend_ref: str,
    target_status: str,
    source_backend_index:    str = '__backend_index__',
    source_parquet_backend:  str = '__parquet_backend__',
    source_sqlite_backend:   str = '__sqlite_backend__',
    source_postgres_backend: str = '__postgres_backend__',
    ) -> TableProcessRef:
    """
    TableProcessRef that writes `source_data` to the backend identified by `source_backend_ref`.
    `source_backend_ref` may include an optional 'if_exists' column to override the backend default.
    Returns `source_backend_ref` as `target_status` unchanged.
    Dispatches to the correct backend via BackendWriteOp.
    """
    return TableProcessRef(
        op_idn  = op_idn,
        source  = [
            source_data,
            source_backend_ref,
            source_backend_index,
            source_parquet_backend,
            source_sqlite_backend,
            source_postgres_backend,
        ],
        target  = [ target_status ],
        terms   = [ 'BackendWriteOp' ],
    )
#/def backend_write_process

def is_parquet_backend_check(
    op_idn: str,
    source_backend_resolved: str,
    ) -> TableCheckRef:
    """TableCheckRef that is True when the resolved backend is __parquet_backend__."""
    return TableCheckRef(
        op_idn = op_idn,
        source = [ source_backend_resolved ],
        terms  = [ 'IsParquetBackendOp' ],
    )
#/def is_parquet_backend_check

def is_sqlite_backend_check(
    op_idn: str,
    source_backend_resolved: str,
    ) -> TableCheckRef:
    """TableCheckRef that is True when the resolved backend is __sqlite_backend__."""
    return TableCheckRef(
        op_idn = op_idn,
        source = [ source_backend_resolved ],
        terms  = [ 'IsSQLiteBackendOp' ],
    )
#/def is_sqlite_backend_check

def is_postgres_backend_check(
    op_idn: str,
    source_backend_resolved: str,
    ) -> TableCheckRef:
    """TableCheckRef that is True when the resolved backend is __postgres_backend__."""
    return TableCheckRef(
        op_idn = op_idn,
        source = [ source_backend_resolved ],
        terms  = [ 'IsPostgresBackendOp' ],
    )
#/def is_postgres_backend_check

