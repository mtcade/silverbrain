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

def _make_process_row( **kwargs ) -> pl.DataFrame:
    """
    Build a single-row DataFrame with the 'table_processes' schema.
    Any column not supplied in kwargs is set to null.
    """
    defaults: dict = {
        'op_id':        None,
        'parent_op_id': None,
        'type':         None,
        'source':       None,
        'target':       None,
        'op':           None,
        'term_ids':     None,
        'condition':    None,
        'process':      None,
        'max_iter':     None,
        'count':        None,
        'ifs':          None,
        'thens':        None,
        'otherwise':    None,
    }
    defaults.update( kwargs )
    return pl.DataFrame(
        { k: [ v ] for k, v in defaults.items() },
        schema = table_schemas[ 'table_processes' ],
    )
#/def _make_process_row

# -- DataFrame interpreter helpers

def _apply_process(
    rows: dict,
    op_id: str,
    tables: dict[ str, 'pl.DataFrame' ],
    tableOps: 'types.TableProcessDict',
    verbose: int = 0,
    verbose_prefix: str = '',
    ) -> 'tuple[ pl.DataFrame, ... ]':
    row = rows[ op_id ]
    t   = row[ 'type' ]

    if t == 'TableProcessRef':
        return tableOps[ row[ 'op' ] ](
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
        local    = dict( tables )
        max_iter = row[ 'max_iter' ] if row[ 'max_iter' ] is not None else 100
        iterations = 0
        while _apply_check( rows, row[ 'condition' ], local, tableOps, verbose, verbose_prefix + "  " ):
            if iterations >= max_iter:
                raise Exception( "reached max_iter={}".format( max_iter ) )
            result = _apply_process( rows, row[ 'process' ], local, tableOps, verbose, verbose_prefix + "  " )
            local |= dict( zip( rows[ row[ 'process' ] ][ 'target' ], result ) )
            iterations += 1
        #/while
        return tuple( local[ tid ] for tid in row[ 'target' ] )
    #
    elif t == 'TableProcessCount':
        local = dict( tables )
        for _ in range( row[ 'count' ] ):
            result = _apply_process( rows, row[ 'process' ], local, tableOps, verbose, verbose_prefix + "  " )
            local |= dict( zip( rows[ row[ 'process' ] ][ 'target' ], result ) )
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
            if row[ 'otherwise' ] is not None:
                result = _apply_process( rows, row[ 'otherwise' ], local, tableOps, verbose, verbose_prefix + "  " )
                local |= dict( zip( rows[ row[ 'otherwise' ] ][ 'target' ], result ) )
        #/for
        return tuple( local[ tid ] for tid in row[ 'target' ] )
    #
    else:
        raise ValueError( "Unknown type: {}".format( t ) )
    #
#/def _apply_process

def _apply_check(
    rows: dict,
    op_id: str,
    tables: dict[ str, 'pl.DataFrame' ],
    tableOps: 'types.TableProcessDict',
    verbose: int = 0,
    verbose_prefix: str = '',
    ) -> bool:
    row = rows[ op_id ]
    return tableOps[ row[ 'op' ] ](
        dfs            = tuple( tables[ tid ] for tid in row[ 'source' ] ),
        verbose        = verbose,
        verbose_prefix = verbose_prefix,
    )
#/def _apply_check

def run_from_df(
    df: 'pl.DataFrame',
    op_id: str,
    dfs: 'tuple[ pl.DataFrame, ... ]',
    tableOps: 'types.TableProcessDict',
    verbose: int = 0,
    verbose_prefix: str = '',
    ) -> 'tuple[ pl.DataFrame, ... ]':
    rows     = { r[ 'op_id' ]: r for r in df.to_dicts() }
    top_row  = rows[ op_id ]
    tables   = dict( zip( top_row[ 'source' ], dfs ) )
    return _apply_process( rows, op_id, tables, tableOps, verbose, verbose_prefix )
#/def run_from_df

# -- Process Ref; Atomic types.TaggedTableProcess

@dataclass
class TableProcessRef():
    source: Sequence[ str ]
    target: Sequence[ str ]
    op: str | types.TableProcess
    opId: str
    """
        Holds a reference to an atomic types.TableProcess. All together acts as an atomic types.TaggedTableProcess
    """
    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        tableOps: types.TableProcessDict,
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> dict[ str, pl.DataFrame ]:
        
        tableProcess: types.TableProcess
        if isinstance( self.op, str ):
            tableProcess = tableOps[ self.op ]
        #
        else:
            tableProcess = self.op
        #
        
        if verbose > 0:
            print(
                verbose_prefix + self.opId
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

    def get_op_bindings( self: Self, process_id: str ) -> pl.DataFrame:
        return pl.DataFrame(
            [{
                'process_id':    process_id,
                'op_id':         self.opId,
                'input_tables':  list( self.source ),
                'output_tables': list( self.target ),
            }],
            schema = table_schemas['op_bindings'],
        )
    #/def get_op_bindings

    def as_polars(
        self: Self,
        parent_op_id: str | None = None,
        ) -> pl.DataFrame:
        return _make_process_row(
            op_id        = self.opId,
            parent_op_id = parent_op_id,
            type         = 'TableProcessRef',
            source       = list( self.source ),
            target       = list( self.target ),
            op           = self.op if isinstance( self.op, str ) else None,
        )
    #/def as_polars
#/class TableProcessRef

# -- Process Ref Shortcuts

def get(
    source: tuple[ str,... ] | str,
    target: tuple[ str,... ] | str,
    opId: str = '_anonymous_simpleGetOp',
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
        op = SimpleGetOp(),
        opId = opId,
    )
#/def get

def transform(
    source: tuple[ str,... ] | str,
    target: tuple[ str,... ] | str,
    lam: types.TableProcess,
    opId: str = '_anonymous_transformOp',
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
        op = TransformOp(
            lam = lam,
        ),
        opId = opId,
    )
#/def transform

@dataclass
class TableCheckRef():
    """
        Used mostly for `TableProcessWhile`. `TableProcessBranch`
    """
    source: Sequence[ str ]
    op: types.TableCheck | str
    opId: str
    
    def __call__(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        tableOps: types.TableProcessDict,
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> bool:
        
        tableCheck: types.TableCheck
        if isinstance( self.op, str ):
            tableCheck = tableOps[ self.op ]
        #
        else:
            tableCheck = self.op
        #
        
        check: bool = tableCheck(
            dfs,
            verbose = verbose,
            verbose_prefix = verbose_prefix,
        )
        
        if verbose > 0:
            print(
                verbose_prefix + self.opId + ': {}'.format(
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

    def get_op_bindings( self: Self, process_id: str ) -> pl.DataFrame:
        return pl.DataFrame(
            [{
                'process_id':    process_id,
                'op_id':         self.opId,
                'input_tables':  list( self.source ),
                'output_tables': [],
            }],
            schema = table_schemas['op_bindings'],
        )
    #/def get_op_bindings

    def as_polars(
        self: Self,
        parent_op_id: str | None = None,
        ) -> pl.DataFrame:
        return _make_process_row(
            op_id        = self.opId,
            parent_op_id = parent_op_id,
            type         = 'TableCheckRef',
            source       = list( self.source ),
            target       = [],
            op           = self.op if isinstance( self.op, str ) else None,
        )
    #/def as_polars
#/class TableCheckRef

def always_true( opId: str = '_always_true' ) -> TableCheckRef:
    """Return a TableCheckRef whose check always returns True."""
    return TableCheckRef(
        source = [],
        op     = lambda dfs, verbose=0, verbose_prefix='': True,
        opId   = opId,
    )
#/def always_true

def always_false( opId: str = '_always_false' ) -> TableCheckRef:
    """Return a TableCheckRef whose check always returns False."""
    return TableCheckRef(
        source = [],
        op     = lambda dfs, verbose=0, verbose_prefix='': False,
        opId   = opId,
    )
#/def always_false

@dataclass
class TableProcessSequence():
    source: Sequence[ str ]
    target: Sequence[ str ]
    terms: Sequence[ types.TaggedTableProcess ]
    opId: str
    
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
                    verbose_prefix + self.opId + " ({}/{})".format(
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

    def get_op_bindings( self: Self, process_id: str ) -> pl.DataFrame:
        return pl.concat(
            [ term.get_op_bindings( process_id ) for term in self.terms ],
            how = 'vertical',
        )
    #/def get_op_bindings

    def as_polars(
        self: Self,
        parent_op_id: str | None = None,
        ) -> pl.DataFrame:
        self_row = _make_process_row(
            op_id        = self.opId,
            parent_op_id = parent_op_id,
            type         = 'TableProcessSequence',
            source       = list( self.source ),
            target       = list( self.target ),
            term_ids     = [ t.opId for t in self.terms ],
        )
        return pl.concat(
            [ self_row ] + [ t.as_polars( self.opId ) for t in self.terms ],
            how = 'vertical',
        )
    #/def as_polars
#/class TableProcessSequence

@dataclass
class TableProcessWhile():
    source: Sequence[ str ]
    target: Sequence[ str ]
    condition: types.TaggedTableCheck
    process: types.TaggedTableProcess
    opId: str
    maxIter: int = 100

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
                    verbose_prefix + self.opId + " ({})".format( iterations )
                )
            #
            if iterations >= self.maxIter:
                raise Exception(
                    "reached maxIter={}".format( self.maxIter )
                )
            #/if iterations >= self.maxIter

            result: tuple[ pl.DataFrame,... ] = self.process(
                dfs = tuple(
                    tables[ tableId ]
                    for tableId in self.process.source
                ),
                tableOps = tableOps,
                verbose = verbose,
                verbose_prefix = verbose_prefix + "  ",
            )

            tables |= dict(
                zip( self.process.target, result )
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
            | self.process.get_signatureCollector()
        )
    #/def get_signatureCollector

    def get_op_bindings( self: Self, process_id: str ) -> pl.DataFrame:
        return pl.concat(
            [
                self.condition.get_op_bindings( process_id ),
                self.process.get_op_bindings( process_id ),
            ],
            how = 'vertical',
        )
    #/def get_op_bindings

    def as_polars(
        self: Self,
        parent_op_id: str | None = None,
        ) -> pl.DataFrame:
        self_row = _make_process_row(
            op_id        = self.opId,
            parent_op_id = parent_op_id,
            type         = 'TableProcessWhile',
            source       = list( self.source ),
            target       = list( self.target ),
            condition    = self.condition.opId,
            process      = self.process.opId,
            max_iter     = self.maxIter,
        )
        return pl.concat(
            [
                self_row,
                self.condition.as_polars( self.opId ),
                self.process.as_polars( self.opId ),
            ],
            how = 'vertical',
        )
    #/def as_polars
#/class TableProcessWhile

@dataclass
class TableProcessCount():
    source: Sequence[ str ]
    target: Sequence[ str ]
    count: int
    process: types.TaggedTableProcess
    opId: str

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
                    verbose_prefix + self.opId + " ({}/{})".format(
                        j+1, self.count
                    )
                )
            #

            result: tuple[ pl.DataFrame,... ] = self.process(
                dfs = tuple(
                    tables[ tableId ]
                    for tableId in self.process.source
                ),
                tableOps = tableOps,
                verbose = verbose,
                verbose_prefix = verbose_prefix + "  ",
            )

            tables |= dict(
                zip( self.process.target, result )
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
        return self.process.get_signatureCollector()
    #/def get_signatureCollector

    def get_op_bindings( self: Self, process_id: str ) -> pl.DataFrame:
        return self.process.get_op_bindings( process_id )
    #/def get_op_bindings

    def as_polars(
        self: Self,
        parent_op_id: str | None = None,
        ) -> pl.DataFrame:
        self_row = _make_process_row(
            op_id        = self.opId,
            parent_op_id = parent_op_id,
            type         = 'TableProcessCount',
            source       = list( self.source ),
            target       = list( self.target ),
            process      = self.process.opId,
            count        = self.count,
        )
        return pl.concat(
            [
                self_row,
                self.process.as_polars( self.opId ),
            ],
            how = 'vertical',
        )
    #/def as_polars
#/class TableProcessCount

@dataclass
class TableProcessBranch():
    opId: str
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
                    verbose_prefix + self.opId + " ({}/{})".format(
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

    def get_op_bindings( self: Self, process_id: str ) -> pl.DataFrame:
        frames = [
            term.get_op_bindings( process_id )
            for term in self.ifs
        ] + [
            term.get_op_bindings( process_id )
            for term in self.thens
        ] + (
            [ self.otherwise.get_op_bindings( process_id ) ]
            if self.otherwise is not None else []
        )
        return pl.concat( frames, how = 'vertical' )
    #/def get_op_bindings

    def as_polars(
        self: Self,
        parent_op_id: str | None = None,
        ) -> pl.DataFrame:
        self_row = _make_process_row(
            op_id        = self.opId,
            parent_op_id = parent_op_id,
            type         = 'TableProcessBranch',
            source       = list( self.source ),
            target       = list( self.target ),
            ifs          = [ t.opId for t in self.ifs ],
            thens        = [ t.opId for t in self.thens ],
            otherwise    = self.otherwise.opId if self.otherwise is not None else None,
        )
        child_frames = [
            t.as_polars( self.opId ) for t in self.ifs
        ] + [
            t.as_polars( self.opId ) for t in self.thens
        ] + (
            [ self.otherwise.as_polars( self.opId ) ]
            if self.otherwise is not None else []
        )
        return pl.concat( [ self_row ] + child_frames, how = 'vertical' )
    #/def as_polars
#/class TableProcessBranch
