#
#//  tableOps.py
#//  silverbrain
#//
#//  Created by Evan Mason on 3/23/26.
#//

from . import types
from .tableOps import TransformOp
from .schema import table_schemas

import polars as pl

from dataclasses import dataclass
from typing import Self, Sequence, Type

# -- Serialization helper

def _make_process_row( node_id: int, **kwargs ) -> pl.DataFrame:
    """
    Build a single-row DataFrame with the '__table_processes__' schema.
    Any column not supplied in kwargs is set to null.
    node_id is the unique integer row identifier for tree navigation.
    """
    defaults: dict = {
        'node_id': node_id,
        'op_id':      None,
        'parent_id':  None,
        'type':       None,
        'source':     None,
        'target':     None,
        'term_ids':   None,
        'condition':  None,
        'count':      None,
        'ifs':        None,
        'thens':      None,
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

# -- DataFrame interpreter helpers

def _apply_process(
    rows: dict,
    pid: int,
    tables: dict[ str, 'pl.DataFrame' ],
    tableOps: 'types.TableProcessDict',
    verbose: int = 0,
    verbose_prefix: str = '',
    ) -> 'tuple[ pl.DataFrame, ... ]':
    row = rows[ pid ]
    t   = row[ 'type' ]

    if t == 'TableProcessRef':
        op_row = rows[ row[ 'term_ids' ][ 0 ] ]
        return tableOps[ op_row[ 'op_id' ] ](
            dfs            = tuple( tables[ tid ] for tid in row[ 'source' ] ),
            verbose        = verbose,
            verbose_prefix = verbose_prefix,
        )
    #
    elif t == 'TableProcessSequence':
        local = dict( tables )
        for term_id in row[ 'term_ids' ]:
            result = _apply_process( rows, term_id, local, tableOps, verbose, verbose_prefix + "  " )
            local |= dict( zip( rows[ term_id ][ 'target' ], result ) )
        #/for term_id
        return tuple( local[ tid ] for tid in row[ 'target' ] )
    #
    elif t == 'TableProcessWhile':
        local      = dict( tables )
        proc_id    = row[ 'term_ids' ][ 0 ]
        max_iter   = row[ 'count' ] if row[ 'count' ] is not None else 100
        iterations = 0
        while _apply_check( rows, row[ 'condition' ], local, tableOps, verbose, verbose_prefix + "  " ):
            if iterations >= max_iter:
                raise Exception( "reached max_iter={}".format( max_iter ) )
            result = _apply_process( rows, proc_id, local, tableOps, verbose, verbose_prefix + "  " )
            local |= dict( zip( rows[ proc_id ][ 'target' ], result ) )
            iterations += 1
        #/while
        return tuple( local[ tid ] for tid in row[ 'target' ] )
    #
    elif t == 'TableProcessCount':
        local   = dict( tables )
        proc_id = row[ 'term_ids' ][ 0 ]
        for _ in range( row[ 'count' ] ):
            result = _apply_process( rows, proc_id, local, tableOps, verbose, verbose_prefix + "  " )
            local |= dict( zip( rows[ proc_id ][ 'target' ], result ) )
        #/for
        return tuple( local[ tid ] for tid in row[ 'target' ] )
    #
    elif t == 'TableProcessBranch':
        local = dict( tables )
        for if_id, then_id in zip( row[ 'ifs' ], row[ 'thens' ] ):
            if _apply_check( rows, if_id, local, tableOps, verbose, verbose_prefix + "  " ):
                result = _apply_process( rows, then_id, local, tableOps, verbose, verbose_prefix + "  " )
                local |= dict( zip( rows[ then_id ][ 'target' ], result ) )
                break
            #/if
        else:
            if row[ 'term_ids' ]:
                otherwise_id = row[ 'term_ids' ][ 0 ]
                result = _apply_process( rows, otherwise_id, local, tableOps, verbose, verbose_prefix + "  " )
                local |= dict( zip( rows[ otherwise_id ][ 'target' ], result ) )
        #/for
        return tuple( local[ tid ] for tid in row[ 'target' ] )
    #
    else:
        raise ValueError( "Unknown type: {}".format( t ) )
    #
#/def _apply_process

def _apply_check(
    rows: dict,
    pid: int,
    tables: dict[ str, 'pl.DataFrame' ],
    tableOps: 'types.TableProcessDict',
    verbose: int = 0,
    verbose_prefix: str = '',
    ) -> bool:
    row    = rows[ pid ]
    op_row = rows[ row[ 'term_ids' ][ 0 ] ]
    return tableOps[ op_row[ 'op_id' ] ](
        dfs            = tuple( tables[ tid ] for tid in row[ 'source' ] ),
        verbose        = verbose,
        verbose_prefix = verbose_prefix,
    )
#/def _apply_check

def find_root_pid( df: 'pl.DataFrame', op_id: str ) -> int:
    """Find the node_id of the root row with the given op_id (parent_id is null)."""
    matches = df.filter(
        ( pl.col( 'op_id' ) == op_id ) & pl.col( 'parent_id' ).is_null()
    ).to_dicts()
    if not matches:
        raise KeyError( "No root process with op_id={!r}".format( op_id ) )
    return matches[ 0 ][ 'node_id' ]
#/def find_root_pid

def reindex_process_df(
    df: pl.DataFrame,
    offset: int,
) -> pl.DataFrame:
    """Add offset to every node_id reference in df (for collision-free merge)."""
    return df.with_columns(
        [
            pl.col( 'node_id' ) + offset,
            pl.col( 'parent_id' ) + offset,
            pl.col( 'term_ids' ).list.eval( pl.element() + offset ),
            pl.col( 'condition' ) + offset,
            pl.col( 'ifs' ).list.eval( pl.element() + offset ),
            pl.col( 'thens' ).list.eval( pl.element() + offset ),
        ]
    )
#/def reindex_process_df

def embed_process_df(
    process_df: pl.DataFrame,
    parent_id:  int | None,
    start_id:   int,
) -> tuple[ pl.DataFrame, int ]:
    """
    Reindex a start_id=0 process DataFrame to start at `start_id` and set the
    root row's parent_id.  Returns (reindexed_df, next_free_id).

    Mirrors the behaviour of TaggedTableProcess.as_polars(parent_id=parent_id, start_id=start_id)
    for an already-serialized DataFrame.
    """
    reindexed = reindex_process_df( process_df, offset = start_id )
    # reindex_process_df adds start_id to every parent_id; the root row's original
    # parent_id is null so null + start_id = null — still null after reindexing.
    # Set it to the caller-supplied parent_id explicitly.
    patched = reindexed.with_columns(
        pl.when( pl.col( 'node_id' ) == start_id )
          .then( pl.lit( parent_id ) )
          .otherwise( pl.col( 'parent_id' ) )
          .alias( 'parent_id' )
    )
    return patched, start_id + len( process_df )
#/def embed_process_df

def run_from_df(
    df: pl.DataFrame,
    root_pid: int,
    dfs: tuple[ pl.DataFrame, ... ],
    tableOps: types.TableProcessDict,
    verbose: int = 0,
    verbose_prefix: str = '',
    ) -> tuple[ pl.DataFrame, ... ]:
    rows     = { r[ 'node_id' ]: r for r in df.to_dicts() }
    top_row  = rows[ root_pid ]
    tables   = dict( zip( top_row[ 'source' ], dfs ) )
    return _apply_process( rows, root_pid, tables, tableOps, verbose, verbose_prefix )
#/def run_from_df

# -- Process Ref; Atomic types.TaggedTableProcess

@dataclass
class TableProcessRef():
    source: Sequence[ str ]
    target: Sequence[ str ]
    terms: Sequence[ str | types.TableProcess ]
    op_id: str
    """
        Holds a reference to an atomic types.TableProcess. All together acts as an atomic types.TaggedTableProcess.
        terms is a single-element list: [op_key_string] or [callable].
    """
    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        tableOps: types.TableProcessDict,
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> dict[ str, pl.DataFrame ]:

        tableProcess: types.TableProcess
        op = self.terms[ 0 ]
        if isinstance( op, str ):
            tableProcess = tableOps[ op ]
        #
        else:
            tableProcess = op
        #

        if verbose > 0:
            print(
                verbose_prefix + self.op_id
            )
            print(
                verbose_prefix + "  {}".format( self.source )
            )
        #/if verbose > 0

        result: tuple[ pl.DataFrame,... ] = tableProcess(
            dfs = dfs,
            verbose = verbose,
            verbose_prefix = verbose_prefix + "    "
        )

        if verbose > 0:
            print(
                verbose_prefix + " >{}".format( self.target )
            )
        #

        if ( resultCount:= len( result ) ) != len( self.target ):
            raise ValueError(
                "Expected {} result tables, got {}".format(
                    len( self.target ), resultCount
                )
            )
        #/if len( targetTables ) != len( processRef.target )

        return result
    #/def __call__

    def get_signatureCollector(
        self: Self,
        ) -> types.SignatureCollector:
        return types.SignatureCollector(
            source = set( self.source ),
            target = set( self.target ),
        )
    #/def get_signatureCollector

    def get_op_bindings( self: Self, op_id: str ) -> pl.DataFrame:
        return pl.DataFrame(
            [{
                'root_op_id':  op_id,
                'op_id':         self.op_id,
                'source':  list( self.source ),
                'target': list( self.target ),
            }],
            schema = table_schemas['op_bindings'],
        )
    #/def get_op_bindings

    def as_polars(
        self: Self,
        parent_id: int | None = None,
        start_id: int = 0,
        ) -> 'tuple[ pl.DataFrame, int ]':
        """Returns (df, next_id) where next_id is the first node_id not yet used."""
        my_id    = start_id
        op       = self.terms[ 0 ]
        child_id = start_id + 1
        self_row = _make_process_row(
            node_id = my_id,
            op_id      = self.op_id,
            parent_id  = parent_id,
            type       = 'TableProcessRef',
            source     = list( self.source ),
            target     = list( self.target ),
            term_ids   = [ child_id ],
        )
        child_row = _make_process_row(
            node_id = child_id,
            parent_id  = my_id,
            type       = 'Op',
            op_id      = op if isinstance( op, str ) else None,
        )
        return pl.concat( [ self_row, child_row ], how = 'vertical' ), start_id + 2
    #/def as_polars
#/class TableProcessRef

# -- Process Ref Shortcuts

def get(
    source: tuple[ str,... ] | str,
    target: tuple[ str,... ] | str,
    op_id: str = '_anonymous_simpleGetOp',
    ) -> TableProcessRef:
    if isinstance( source, str ):
        source = ( source, )
    #
    if isinstance( target, str ):
        target = ( target, )
    #

    return TableProcessRef(
        source = source,
        target = target,
        terms  = [ SimpleGetOp() ],
        op_id  = op_id,
    )
#/def get

def transform(
    source: tuple[ str,... ] | str,
    target: tuple[ str,... ] | str,
    lam: types.TableProcess,
    op_id: str = '_anonymous_transformOp',
    ) -> TransformOp:
    if isinstance( source, str ):
        source = ( source, )
    #
    if isinstance( target, str ):
        target = ( target, )
    #

    return TableProcessRef(
        source = source,
        target = target,
        terms  = [ TransformOp( lam = lam ) ],
        op_id  = op_id,
    )
#/def transform

@dataclass
class TableCheckRef():
    """
        Used mostly for `TableProcessWhile`. `TableProcessBranch`.
        terms is a single-element list: [op_key_string] or [callable].
    """
    source: Sequence[ str ]
    terms: Sequence[ types.TableCheck | str ]
    op_id: str

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        tableOps: types.TableProcessDict,
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> bool:

        tableCheck: types.TableCheck
        op = self.terms[ 0 ]
        if isinstance( op, str ):
            tableCheck = tableOps[ op ]
        #
        else:
            tableCheck = op
        #

        check: bool = tableCheck(
            dfs,
            verbose = verbose,
            verbose_prefix = verbose_prefix,
        )

        if verbose > 0:
            print(
                verbose_prefix + self.op_id + ': {}'.format(
                    check
                )
            )
        #

        return check
    #/def __call__

    def get_signatureCollector(
        self: Self,
        ) -> types.SignatureCollector:
        return types.SignatureCollector(
            source = set( self.source ),
            target = set(),
        )
    #/def get_signatureCollector

    def get_op_bindings( self: Self, op_id: str ) -> pl.DataFrame:
        return pl.DataFrame(
            [{
                'root_op_id':  op_id,
                'op_id':         self.op_id,
                'source':  list( self.source ),
                'target': [],
            }],
            schema = table_schemas['op_bindings'],
        )
    #/def get_op_bindings

    def as_polars(
        self: Self,
        parent_id: int | None = None,
        start_id: int = 0,
        ) -> 'tuple[ pl.DataFrame, int ]':
        """Returns (df, next_id) where next_id is the first node_id not yet used."""
        my_id    = start_id
        op       = self.terms[ 0 ]
        child_id = start_id + 1
        self_row = _make_process_row(
            node_id = my_id,
            op_id      = self.op_id,
            parent_id  = parent_id,
            type       = 'TableCheckRef',
            source     = list( self.source ),
            target     = [],
            term_ids   = [ child_id ],
        )
        child_row = _make_process_row(
            node_id = child_id,
            parent_id  = my_id,
            type       = 'Op',
            op_id      = op if isinstance( op, str ) else None,
        )
        return pl.concat( [ self_row, child_row ], how = 'vertical' ), start_id + 2
    #/def as_polars
#/class TableCheckRef

def always_true( op_id: str = '_always_true' ) -> TableCheckRef:
    """Return a TableCheckRef whose check always returns True."""
    return TableCheckRef(
        source = [],
        terms  = [ lambda dfs, verbose=0, verbose_prefix='': True ],
        op_id  = op_id,
    )
#/def always_true

def always_false( op_id: str = '_always_false' ) -> TableCheckRef:
    """Return a TableCheckRef whose check always returns False."""
    return TableCheckRef(
        source = [],
        terms  = [ lambda dfs, verbose=0, verbose_prefix='': False ],
        op_id  = op_id,
    )
#/def always_false

@dataclass
class TableProcessSequence():
    source: Sequence[ str ]
    target: Sequence[ str ]
    terms: Sequence[ types.TaggedTableProcess ]
    op_id: str

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        tableOps: types.TableProcessDict,
        verbose: int = 0,
        verbose_prefix: str = "",
        ) -> tuple[ pl.DataFrame, ... ]:

        tables: dict[ str, pl.DataFrame ] = dict(
            zip( self.source, dfs )
        )

        taggedProcess: types.TaggedTableProcess
        result: tuple[ pl.DataFrame,... ]
        for j in range( len( self.terms ) ):
            taggedProcess = self.terms[j]
            if verbose > 0:
                print(
                    verbose_prefix + self.op_id + " ({}/{})".format(
                        j+1, len( self.terms )
                    )
                )
            #

            result = taggedProcess(
                dfs = tuple(
                    tables[ tableId ]
                    for tableId in taggedProcess.source
                ),
                tableOps = tableOps,
                verbose = verbose,
                verbose_prefix = verbose_prefix + "  ",
            )

            tables |= dict(
                zip(
                    taggedProcess.target,
                    result,
                )
            )
        #/for j in range( len( self.terms ) )

        return tuple(
            tables[ targetId ]
            for targetId in self.target
        )
    #/def __call__

    def get_signatureCollector(
        self: Self,
        ) -> types.SignatureCollector:
        signatureCollector: types.SignatureCollector = types.SignatureCollector()

        for term in self.terms:
            signatureCollector |= term.get_signatureCollector()
        #

        return signatureCollector
    #/def get_signatureCollector

    def get_op_bindings( self: Self, op_id: str ) -> pl.DataFrame:
        return pl.concat(
            [ term.get_op_bindings( op_id ) for term in self.terms ],
            how = 'vertical',
        )
    #/def get_op_bindings

    def as_polars(
        self: Self,
        parent_id: int | None = None,
        start_id: int = 0,
        ) -> 'tuple[ pl.DataFrame, int ]':
        """Returns (df, next_id) where next_id is the first node_id not yet used."""
        my_id   = start_id
        next_id = start_id + 1
        frames  = []
        term_ids = []
        for term in self.terms:
            term_id = next_id
            term_df, next_id = term.as_polars( parent_id = my_id, start_id = term_id )
            term_ids.append( term_id )
            frames.append( term_df )
        #/for
        self_row = _make_process_row(
            node_id = my_id,
            op_id      = self.op_id,
            parent_id  = parent_id,
            type       = 'TableProcessSequence',
            source     = list( self.source ),
            target     = list( self.target ),
            term_ids   = term_ids,
        )
        return pl.concat( [ self_row ] + frames, how = 'vertical' ), next_id
    #/def as_polars
#/class TableProcessSequence

@dataclass
class TableProcessWhile():
    source: Sequence[ str ]
    target: Sequence[ str ]
    condition: types.TaggedTableCheck
    terms: Sequence[ types.TaggedTableProcess ]
    op_id: str
    maxIter: int = 2**31 - 1

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        tableOps: types.TableProcessDict,
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame,... ]:

        tables: dict[ str, pl.DataFrame ] = dict(
            zip( self.source, dfs )
        )

        iterations: int = 0

        while self.condition(
            dfs = tuple(
                tables[ tableId ]
                for tableId in self.condition.source
            ),
            tableOps = tableOps,
            verbose = verbose,
            verbose_prefix = verbose_prefix + "  ",
        ):
            if verbose > 0:
                print(
                    verbose_prefix + self.op_id + " ({})".format( iterations )
                )
            #
            if iterations >= self.maxIter:
                raise Exception(
                    "reached maxIter={}".format( self.maxIter )
                )
            #/if iterations >= self.maxIter

            result: tuple[ pl.DataFrame,... ] = self.terms[ 0 ](
                dfs = tuple(
                    tables[ tableId ]
                    for tableId in self.terms[ 0 ].source
                ),
                tableOps = tableOps,
                verbose = verbose,
                verbose_prefix = verbose_prefix + "  ",
            )

            tables |= dict(
                zip( self.terms[ 0 ].target, result )
            )

            iterations += 1
        #/while self.condition(...)

        return tuple(
            tables[ targetId ]
            for targetId in self.target
        )
    #/def __call__

    def get_signatureCollector(
        self: Self,
        ) -> types.SignatureCollector:
        return (
            self.condition.get_signatureCollector()
            | self.terms[ 0 ].get_signatureCollector()
        )
    #/def get_signatureCollector

    def get_op_bindings( self: Self, op_id: str ) -> pl.DataFrame:
        return pl.concat(
            [
                self.condition.get_op_bindings( op_id ),
                self.terms[ 0 ].get_op_bindings( op_id ),
            ],
            how = 'vertical',
        )
    #/def get_op_bindings

    def as_polars(
        self: Self,
        parent_id: int | None = None,
        start_id: int = 0,
        ) -> 'tuple[ pl.DataFrame, int ]':
        """Returns (df, next_id) where next_id is the first node_id not yet used."""
        my_id    = start_id
        cond_id  = start_id + 1
        cond_df, next_id = self.condition.as_polars( parent_id = my_id, start_id = cond_id )
        proc_id  = next_id
        proc_df, next_id = self.terms[ 0 ].as_polars( parent_id = my_id, start_id = proc_id )
        self_row = _make_process_row(
            node_id = my_id,
            op_id      = self.op_id,
            parent_id  = parent_id,
            type       = 'TableProcessWhile',
            source     = list( self.source ),
            target     = list( self.target ),
            condition  = cond_id,
            term_ids   = [ proc_id ],
            count      = self.maxIter,
        )
        return pl.concat( [ self_row, cond_df, proc_df ], how = 'vertical' ), next_id
    #/def as_polars
#/class TableProcessWhile

@dataclass
class TableProcessCount():
    source: Sequence[ str ]
    target: Sequence[ str ]
    count: int
    terms: Sequence[ types.TaggedTableProcess ]
    op_id: str

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        tableOps: types.TableProcessDict,
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame,... ]:

        tables: dict[ str, pl.DataFrame ] = dict(
            zip( self.source, dfs )
        )

        for j in range( self.count ):
            if verbose > 0:
                print(
                    verbose_prefix + self.op_id + " ({}/{})".format(
                        j+1, self.count
                    )
                )
            #

            result: tuple[ pl.DataFrame,... ] = self.terms[ 0 ](
                dfs = tuple(
                    tables[ tableId ]
                    for tableId in self.terms[ 0 ].source
                ),
                tableOps = tableOps,
                verbose = verbose,
                verbose_prefix = verbose_prefix + "  ",
            )

            tables |= dict(
                zip( self.terms[ 0 ].target, result )
            )
        #/for j in range( self.count )

        return tuple(
            tables[ targetId ]
            for targetId in self.target
        )
    #/def __call__

    def get_signatureCollector(
        self: Self,
        ) -> types.SignatureCollector:
        return self.terms[ 0 ].get_signatureCollector()
    #/def get_signatureCollector

    def get_op_bindings( self: Self, op_id: str ) -> pl.DataFrame:
        return self.terms[ 0 ].get_op_bindings( op_id )
    #/def get_op_bindings

    def as_polars(
        self: Self,
        parent_id: int | None = None,
        start_id: int = 0,
        ) -> 'tuple[ pl.DataFrame, int ]':
        """Returns (df, next_id) where next_id is the first node_id not yet used."""
        my_id   = start_id
        proc_id = start_id + 1
        proc_df, next_id = self.terms[ 0 ].as_polars( parent_id = my_id, start_id = proc_id )
        self_row = _make_process_row(
            node_id = my_id,
            op_id      = self.op_id,
            parent_id  = parent_id,
            type       = 'TableProcessCount',
            source     = list( self.source ),
            target     = list( self.target ),
            term_ids   = [ proc_id ],
            count      = self.count,
        )
        return pl.concat( [ self_row, proc_df ], how = 'vertical' ), next_id
    #/def as_polars
#/class TableProcessCount

@dataclass
class TableProcessBranch():
    op_id: str
    source: Sequence[ str ]
    target: Sequence[ str ]
    ifs: Sequence[ types.TaggedTableCheck ]
    thens: Sequence[ types.TaggedTableProcess ]
    otherwise: types.TaggedTableProcess | None = None

    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        tableOps: types.TableProcessDict,
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame,... ]:

        tables: dict[ str, pl.DataFrame ] = dict(
            zip( self.source, dfs )
        )

        j: int = 0

        while j < len( self.ifs ):
            if verbose > 0:
                print(
                    verbose_prefix + self.op_id + " ({}/{})".format(
                        j+1, len( self.ifs )
                    )
                )
            #

            check: bool = self.ifs[j](
                dfs = tuple(
                    tables[ tableId ]
                    for tableId in self.ifs[j].source
                ),
                tableOps = tableOps,
                verbose = verbose,
                verbose_prefix = verbose_prefix + "  ",
            )

            if check:
                result: tuple[ pl.DataFrame,... ] = self.thens[j](
                    dfs = tuple(
                        tables[ tableId ]
                        for tableId in self.thens[j].source
                    ),
                    tableOps = tableOps,
                    verbose = verbose,
                    verbose_prefix = verbose_prefix + "  ",
                )

                tables |= dict(
                    zip( self.thens[j].target, result )
                )

                break
            #/if check
            else:
                j += 1
            #
        #/while j < len( self.ifs )

        if j >= len( self.ifs ) and self.otherwise is not None:
            if verbose > 0:
                print(
                    verbose_prefix + " (Otherwise)"
                )
            #

            result: tuple[ pl.DataFrame,... ] = self.otherwise(
                dfs = tuple(
                    tables[ tableId ]
                    for tableId in self.otherwise.source
                ),
                tableOps = tableOps,
                verbose = verbose,
                verbose_prefix = verbose_prefix + "  ",
            )

            tables |= dict(
                zip( self.otherwise.target, result )
            )
        #/if j >= len( self.ifs ) and self.otherwise is not None

        return tuple(
            tables[ targetId ]
            for targetId in self.target
        )
    #/def __call__

    def get_signatureCollector(
        self: Self,
        ) -> types.SignatureCollector:
        signatureCollector: types.SignatureCollector = (
            types.SignatureCollector()
            if self.otherwise is None
            else self.otherwise.get_signatureCollector()
        )

        for term in self.ifs:
            signatureCollector |= term.get_signatureCollector()
        #

        for term in self.thens:
            signatureCollector |= term.get_signatureCollector()
        #

        return signatureCollector
    #/def get_signatureCollector

    def get_op_bindings( self: Self, op_id: str ) -> pl.DataFrame:
        frames = [
            term.get_op_bindings( op_id )
            for term in self.ifs
        ] + [
            term.get_op_bindings( op_id )
            for term in self.thens
        ] + (
            [ self.otherwise.get_op_bindings( op_id ) ]
            if self.otherwise is not None else []
        )
        return pl.concat( frames, how = 'vertical' )
    #/def get_op_bindings

    def as_polars(
        self: Self,
        parent_id: int | None = None,
        start_id: int = 0,
        ) -> 'tuple[ pl.DataFrame, int ]':
        """Returns (df, next_id) where next_id is the first node_id not yet used."""
        my_id   = start_id
        next_id = start_id + 1
        frames  = []
        if_ids   = []
        then_ids = []
        for if_term in self.ifs:
            if_id = next_id
            if_df, next_id = if_term.as_polars( parent_id = my_id, start_id = if_id )
            if_ids.append( if_id )
            frames.append( if_df )
        #/for
        for then_term in self.thens:
            then_id = next_id
            then_df, next_id = then_term.as_polars( parent_id = my_id, start_id = then_id )
            then_ids.append( then_id )
            frames.append( then_df )
        #/for
        otherwise_term_ids = None
        if self.otherwise is not None:
            otherwise_id = next_id
            ow_df, next_id = self.otherwise.as_polars( parent_id = my_id, start_id = otherwise_id )
            frames.append( ow_df )
            otherwise_term_ids = [ otherwise_id ]
        #/if
        self_row = _make_process_row(
            node_id = my_id,
            op_id      = self.op_id,
            parent_id  = parent_id,
            type       = 'TableProcessBranch',
            source     = list( self.source ),
            target     = list( self.target ),
            ifs        = if_ids,
            thens      = then_ids,
            term_ids   = otherwise_term_ids,
        )
        return pl.concat( [ self_row ] + frames, how = 'vertical' ), next_id
    #/def as_polars
#/class TableProcessBranch
