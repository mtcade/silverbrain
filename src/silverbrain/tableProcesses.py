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
#/class TableCheckRef

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
#/class TableProcessCount

@dataclass
class TableProcessBranch():
    source: Sequence[ str ]
    target: Sequence[ str ]
    ifs: Sequence[ types.TaggedTableCheck | bool ]
    thens: Sequence[ types.TaggedTableProcess ]
    otherwise: types.TaggedTableProcess | None
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

        j: int = 0

        check: bool
        while j < len( self.ifs ):
            if verbose > 0:
                print(
                    verbose_prefix + self.opId + " ({}/{})".format(
                        j+1, len( self.ifs )
                    )
                )
            #
            if isinstance( self.ifs[j], bool ):
                check = self.ifs[j]
            #
            else:
                check = self.ifs[j](
                    dfs = tuple(
                        tables[ tableId ]
                        for tableId in self.ifs[j].source
                    ),
                    tableOps = tableOps,
                    verbose = verbose,
                    verbose_prefix = verbose_prefix + "  ",
                )
            #

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
            if not isinstance( term, bool ):
                signatureCollector |= term.get_signatureCollector()
            #
        #

        for term in self.thens:
            signatureCollector |= term.get_signatureCollector()
        #

        return signatureCollector
    #/def get_signatureCollector

    def get_op_bindings( self: Self, process_id: str ) -> pl.DataFrame:
        frames = [
            term.get_op_bindings( process_id )
            for term in self.ifs if not isinstance( term, bool )
        ] + [
            term.get_op_bindings( process_id )
            for term in self.thens
        ] + (
            [ self.otherwise.get_op_bindings( process_id ) ]
            if self.otherwise is not None else []
        )
        return pl.concat( frames, how = 'vertical' )
    #/def get_op_bindings
#/class TableProcessBranch

