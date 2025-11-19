"""
    Holds multiple cells in a graph; see ./nucleus.py
"""
from . import nucleus
from .types import Cell, CellId

import polars as pl

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Literal, Protocol, Self, Type
from queue import Queue

class CellularWeb( nucleus.Nucleus ):
    """
        Has a series of cells, connected by `.connections`. Must be acyclic as a directed graph.
    """
    def __init__(
        self: Self,
        status: dict[ str, any ] | None = None,
        cells: dict[ CellId, Cell ] | None = None,
        connections: dict[ CellId, CellId ] | None = None,
        inputIds: list[ CellId ] | None = None,
        inbox: Queue | None = None,
        outbox: Queue | None = None,
        log: Queue | None = None
        ) -> None:
        
        super().__init__(
            status = status,
            inbox = inbox,
            outbox = outbox,
            log = log
        )
        
        self.cells = cells or {}
        self.connections = connections or {}
        self.inputIds = inputIds or []
        
        return
    #/def __init__
    
    def queueUpdate(
        self: Self,
        update: pl.DataFrame
        ) -> None:
        self.inbox.put( update )
        return
    #/def queueUpdate
    
    def cycleOnce( self: Self ) -> None:
        """
            Has each cell run `.updateAll`, consuming everything in their queues, and pushing results to their outbox. Then, move everything from each outbox to their destination inboxes via `self.connections`
        """
        
        # Have each cell run its entire queue
        for cid, cell in self.cells.items():
            cell.updateAll()
        #/for cid, cell in self.cells.items()
        
        for cid, cell in self.cells.items():
            while not cell.outbox.empty():
                # Get output
                tab: pl.DataFrame = cell.outbox.get()

                # Send output to destinations
                for did in self.connections[ cid ]:
                    self.cells[ did ].queueUpdate( tab )
                #/for did in self.connections[ cid ]
                
                cell.outbox.task_done()
            #/while not cell.outbox.empty()
        #/for cid, cell in self.cells.items()
        
        return
    #/def cycleOnce
    
    def updateOnce( self: Self ) -> None:
        if self.inbox.empty():
            return
        #/if self.inbox.empty()
        
        tab: pl.DataFrame = self.inbox.get()
        self.inbox.task_done()
        
        for cid in self.inputIds:
            self.cells[ cid ].queueUpdate( tab )
        #/for cid in self.inputIds
        
        self.cycleOnce()
        
        return
    #/def updateOnce
    
    def updateAll( self: Self ) -> None:
        """
            Apply all updates in the inbox to the input cell, and for each output
            
            TODO: decide how often to cycle
        """
        if self.inbox.empty():
            return
        #/if self.inbox.empty()
        
        tab: pl.DataFrame
        while not self.inbox.empty():
            tab = self.inbox.get()
            self.inbox.task_done()
            
            for cid in self.inputIds:
                self.cells[ cid ].queueUpdate( tab )
            #/for cid in self.inputIds
        #/while not self.inbox.empty()
        
        self.cycleOnce()
        
        return
    #/def updateAll
#/class CellularWeb

# -- TableOp functions

def add_pl_columns(
    expr: tuple[ pl.Expr ],
    dtype: pl.DataType,
    verbose: int = 0,
    verbose_prefix: str = '',
    ) -> pl.Expr:
    if dtype == pl.List:
        return pl.concat_list( *expr )
    #
    
    if dtype.is_numeric():
        return pl.sum_horizontal( *expr )
    #
    raise TypeError("Unrecognized sum dtype={}".format(dtype))
#/def add_pl_columns

def add_values_from_join(
    df: pl.DataFrame,
    update: pl.DataFrame,
    keys: tuple[ str,... ],
    ignore: tuple[ str ] = (),
    verbose: int = 0,
    verbose_prefix: str = '',
    **kwargs # passed to df.join
    ) -> pl.DataFrame:
    """
        Uses 'addition' from `add_pl_columns(...)` for every column in `update` matching a column in df, except keys which form the join. If the key is not present in `df` then take the raw `update` value.
    """
    ignore_columns: tuple[ str ] = keys + ignore
    parameter_columns: tuple[ str, ... ] = tuple(
        col for col in update.columns if col not in ignore_columns
    )
    
    return df.join(
        update,
        on = keys,
        how = 'left',
        **kwargs
    ).with_columns(
        (
            pl.when(
                pl.col( col +'_right' ).is_null()\
                    | pl.col( col ).is_null()
                #/
            ).then(
                pl.col( col )
            ).otherwise(
                add_pl_columns(
                    (
                        pl.col( col ),
                        pl.col( col +'_right')
                    ),
                    dtype = df.schema[ col ]
                )
            ).alias( col ) if col in df.columns
                # raw update value if not in `df`
                else pl.col( col )\
                for col in parameter_columns
            #/
        )
    ).select(
       set( df.columns + update.columns )
    )
#/def add_values_from_join

def update_df_on_keys(
    df: pl.DataFrame | None,
    update: pl.DataFrame,
    keys: tuple[ str,... ],
    verbose: int = 0,
    verbose_prefix: str = '',
    **kwargs # passed to df.join
    ) -> pl.DataFrame:
    """
        Use a left join and merge to update keys
    """
    
    # If df to update, just set it to the new update
    if df is None:
        return update
    #
    
    # Gather non id columns to join in
    join_columns: tuple[ str, ... ] = tuple(
        col for col in update.columns\
            if col in df.columns\
            and col not in keys
        #/
    )
    
    if not join_columns:
        raise ValueError(
            "Missing columns for update; found `update.columns={}` with `keys={}`".format(
                update.columns,
                keys
            )
        )
    #/if not join_columns
    
    return df.join(
        update.select(
            keys + join_columns
        ).with_row_index( '_index' ),
        on = keys,
        how = 'outer_coalesce',
        **kwargs
    ).with_columns(
        tuple(
            pl.when(
                pl.col('_index').is_null()
            ).then(
                pl.col( col )
            ).otherwise(
                pl.col( col + '_right' )
            ).alias( col )\
                for col in join_columns
            #/
        )
    ).drop(
        tuple(
            col +'_right'\
                for col in join_columns
            #/
        ) + ('_index',)
    )
#/def update_df_on_keys

def update_df_on_keys_by_namerColumn(
    df: pl.DataFrame,
    update: pl.DataFrame,
    keys: tuple[ str ],
    namerCol: str,
    sourceCol: str,
    verbose: int = 0,
    verbose_prefix: str = '',
    **kwargs # passed to df.join
    ) -> pl.DataFrame:
    """
        When we get a join match based on keys, we look at the value in update of `namerCol`. This will be a column in `df`. Set the value of that column to the column `sourceCol` from update.
        
        If you want to have some function instead of `sourceCol`, then make another transformer to make that column.
    """
    # Our namer and source might be in df, so we have to rename them to properly
    #   grab them after the join on keys
    namerCol_join = namerCol + '_right' if namerCol in df.columns else namerCol
    sourceCol_join = sourceCol + '_right' if sourceCol in df.columns else sourceCol
    
    # Get the expression to find the col named by `namerCol`, after joining
    colNamer_expr_tup: tuple[ pl.Expr,... ] = tuple(
        pl.when(
            pl.col( namerCol_join ) == targetCol
        ).then(
            pl.col( sourceCol_join )
        ).otherwise(
            pl.col( targetCol )
        ).alias(
            targetCol
        )\
            for targetCol in update[ namerCol ].drop_nulls().unique()
        #/
    )
    
    return df.join(
        update.select(
            keys + (namerCol,) + (sourceCol,)
        ),
        on = keys,
        how = 'left',
        **kwargs
    ).with_columns(
        colNamer_expr_tup
    ).drop(
        ( sourceCol_join, namerCol_join )
    )
#/def update_df_on_keys_by_namerColumn

# TODO: rename classes
@dataclass
class TableOpParent():
    source: tuple[ CellId,... ]
    target: tuple[ CellId,... ] | None
    
    @abstractmethod
    def process(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame, ... ]:
        ...
    #/def process
#/class TableOpParent
    
@dataclass
class SimpleGetOp( TableOpParent ):
    source: tuple[ CellId, ... ]
    target: None = None
    
    def process(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame, ... ]:
        return dfs
    #/def process
#/class SimpleGetOp

@dataclass
class SimplePrintOp( SimpleGetOp ):
    message: str = ''
    
    def process(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame,... ]:
        if self.message:
            print( self.message )
        #
        for i in range( len( dfs ) ):
            print( self.source[i] )
            print( dfs[i].schema )
            print( dfs[i] )
        #
        return dfs
    #/def process
#/class SimplePrintOp

@dataclass
class AssertOp( SimpleGetOp ):
    lam: Callable[
        [
            tuple[ pl.DataFrame,... ],
        ],
        bool
    ] = lambda dfs: True
    
    def process(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame,... ]:
        assert self.lam( dfs )
        return dfs
    #/def process
#/class AssertOp

@dataclass
class TransformPrintOp( SimplePrintOp ):
    lam: nucleus.TableLambda = lambda dfs: dfs
    
    def process(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame,... ]:
        if self.message:
            print( self.message )
        #
        
        transformedDfs: tuple[ pl.DataFrame,... ] = self.lam(
            dfs, verbose, verbose_prefix
        )
        for i in range( len( transformedDfs ) ):
            print( transformedDfs[i].columns )
            print( transformedDfs[i] )
        #
        
        return dfs
    #/def process
#/class TransformPrintOp

@dataclass
class TableOp( TableOpParent ):
    lam: nucleus.TableLambda
    
    def process(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[ pl.DataFrame,...]:
        return self.lam(
            dfs,
            verbose,
            verbose_prefix,
        )
    #/def process
    
    @classmethod
    def update_target_on_keys(
        cls: Type,
        source: tuple[ CellId,... ],
        target: tuple[ CellId,... ],
        keys: tuple[ str, ... ],
        **kwargs # passed to dfs[0].join
        ) -> Self:
        """
            For a target table, update it using the single source table. (As usual, the input to the lam will be tableIds `tuple(source[0], target)`
        """
        assert len( source ) == 2
        assert len( target ) == 1
        
        return cls(
            source = source,
            target = target,
            lam = lambda dfs, verbose=0, verbose_prefix='': (
                update_df_on_keys(
                    df = dfs[1],
                    update = dfs[0],
                    keys = keys,
                    verbose = verbose,
                    verbose_prefix = verbose_prefix,
                    **kwargs
                ),
            )
        )
    #/def update_target_on_keys
    
    @classmethod
    def update_target_on_keys_by_namerColumn(
        cls: Type,
        source: tuple[ CellId,... ],
        target: tuple[ CellId,... ],
        keys: tuple[ str, ... ],
        namerCol: str,
        sourceCol: str,
        **kwargs # passed to dfs[0].join
        ) -> Self:
        """
            Pattern of updating a column in dfs[1], where the value comes from dfs[0][sourceCol], with a target column from `namerCol`
        """
        assert len( source ) == 2
        assert len( target ) == 1
        
        return cls(
            source = source,
            target = target,
            lam = lambda dfs, verbose=0, verbose_prefix='': (
                update_df_on_keys_by_namerColumn(
                    df = dfs[1],
                    update = dfs[0],
                    keys = keys,
                    namerCol = namerCol,
                    sourceCol = sourceCol,
                    verbose = verbose,
                    verbose_prefix = verbose_prefix,
                    **kwargs
                ),
            )
        )
    #/def update_target_on_keys_by_namerColumn
#/class TableOp

class Web( nucleus.Nucleus ):
    """
        Holds all tables, and has just lambdas to modify the tables in the form of tableSequences
        
        Attributes:
        
        inputIds   The roots of subgraphs which flow from inputs.
        
        tableTopologies:    Dictionary mapping inputIds to a tableTopology
                            rooted by those inputIds. 09-13-2025: Deprecated in favor of
                            `tableSequences`
        
        tables      Referenced by their `CellId`. The `diGraph` tells us the
                    relations between them, but the `Web` itself applies the corresponding operations in `diGraphOps`
                    
    """
    def __init__(
        self: Self,
        status: dict[ str, any ] | None = None,
        inputIds: list[
            str
        ] | None = None,
        tableSequences: dict[
            str,
            list[ TableOp ]
        ] | None = None,
        tables: dict[
            CellId,
            pl.DataFrame | None
        ] | None = None,
        inbox: Queue | None = None,
        outbox: Queue | None = None,
        log: Queue | None = None,
        verbose: int = 0
        ) -> None:
        
        super().__init__(
            status = status,
            inbox = inbox,
            outbox = outbox,
            log = log
        )

        self.inputIds = inputIds or []
        self.tableSequences = tableSequences or {}
        self.tables = tables or {}
        self.verbose = verbose

        return
    #/def __init__
    
    def init_table(
        self: Self,
        df: pl.DataFrame,
        cellId: CellId | None = None
        ) -> None:
        """
            Adds a new table to `self.tables`. If the `cellId` is already present, the value must be `None`.
        """
        if cellId in self.tables:
            assert self.tables[ cellId ] is None
            
        self.tables[ cellId ] = df
        
        return
    #/def init_table
    
    # Processing
    def process_tableSequence(
        self: Self,
        tableSequence: list[ TableOp ],
        dfs: tuple[ pl.DataFrame | None, ... ] = (),
        verbose: int = 0,
        verbose_prefix: str = ''
        ) -> tuple[ pl.DataFrame,... ]:
        """
            Perform a sequence of computations on tables. Sometimes, a `TableOp` will reference a table not in `self.tables`. This means it's a temporary table for calculating; we store it in a dictionary of temporary tables, `_tables` to save memory on the web
            
            Returns the final result. Note that you can also use this as a `get`; Some 'get' will use 'dfs' to make a query. If there is no needed query table, then you can use `df = (None,)` while making sure not to use it with any `TableOp` which references `inputIds`
        """
        
        _tables: dict[ CellId, pl.DataFrame ] = {}
        
        sourceTables: tuple[ pl.DataFrame | None,... ]
        targetTables: tuple[ pl.DataFrame | None,... ]

        # First op must only have `self.tables` references at the end of source, after references to the input `dfs`
        # Thus, the first ids in `tableSequence[0].source` corresponding to `dfs` must not be in `self.tables`
        # The rest must be in `self.tables` since nothing will have been written to `_tables`

        # Write `dfs` to `_tables` in case it's referenced after `tableSequence[0]`
        
        for j in range( len( tableSequence[0].source ) ):
            if not isinstance( tableSequence[0].source, tuple ):
                raise TypeError(
                    "Expected tuple, got `tableSequence[0].source={}`, `type(...)={}`".format(
                        tableSequence[0].source,
                        type(tableSequence[0].source),
                    )
                )
            #
            cellId = tableSequence[0].source[j]
            if j < len(dfs):
                assert cellId not in self.tables
                _tables[ cellId ] = dfs[ j ]
            #
            else:
                if cellId not in self.tables:
                    raise ValueError(
                        "Missing cellId={} from self.tables".format(
                            cellId
                        )
                    )
                #/if cellId not in self.tables
            #/if j < len(dfs)/else
        #/for j in range( len( tableSequence[0].source ) )
        
        op_index: int = 0
        targetIds: tuple[ str ]
        for op in tableSequence:
            if not isinstance( op.source, tuple ):
                raise TypeError(
                    "Expected tuple, got `op.source={}`, `type(...)={}`".format(
                        op.source,
                        type(op.source),
                    )
                )
            #/if not isinstance( op.source, tuple )
            
            sourceTables = tuple(
                self.tables[ cellId ] if cellId in self.tables\
                    else _tables[ cellId ]\
                    for cellId in op.source
                #/
            )

            if verbose > 0:
                print("# " + verbose_prefix + "{}".format(op.source))
            #

            
            if not all(
                isinstance( _table, pl.DataFrame )\
                    for _table in sourceTables
                #/
            ):
                for i in range( len( op.source ) ):
                    if not isinstance(
                        sourceTables[i], pl.DataFrame
                    ):
                        print( sourceTables[i] )
                    #/if not isinstance( ... )
                #/for i in range( len( op.source ) )
                
                raise Exception(
                    "Bad source tables: {}".format(
                        [
                            op.source[i] for i in range( len( op.source ) )\
                                if not isinstance(
                                    sourceTables[i], pl.DataFrame
                                )
                            #/
                        ]
                    )
                )
            #/if not all(...)
            
            targetTables = op.process(
                sourceTables,
                verbose = verbose,
                verbose_prefix = verbose_prefix + '  '
            )
            
            if op.target is None:
                targetIds = op.source
            #
            else:
                if not isinstance( op.target, tuple ):
                    raise TypeError(
                        "Expected tuple, got `op.target={}`, `type(...)={}`".format(
                            op.target,
                            type(op.target),
                        )
                    )
                #/if not { isinstance( op.target, tuple ) }
                targetIds = op.target
            #/switch op.target
            
            if verbose > 0:
                print(
                    "# " + verbose_prefix + "-> {}".format(
                        targetIds
                    )
                )
            #/if verbose > 0
            
            if ( _resultCount:= len( targetTables ) ) != len( targetIds ):
                raise ValueError(
                    "Expected {} result tables, got {}".format(
                        len( targetIds ), _resultCount
                    )
                )
            #
            
            if not all(
                isinstance( _table, pl.DataFrame )\
                    for _table in targetTables
                #/
            ):
                for i in range( _resultCount ):
                    if not isinstance(
                        targetTables[i], pl.DataFrame
                    ):
                        print( targetTables[i] )
                    #/if not isinstance( ... )
                #/for i in range( len( op.source ) )
                
                raise Exception(
                    "Bad target tables: {}".format(
                        [
                            op.target[i] for i in range( _resultCount )\
                                if not isinstance(
                                    targetTables[i], pl.DataFrame
                                )
                            #/
                        ]
                    )
                )
            #/if not all(...)
            for i in range( _resultCount ):
                if targetIds[ i ] in self.tables:
                    self.tables[ targetIds[i] ] = targetTables[ i ]
                #
                else:
                    _tables[ targetIds[i] ] = targetTables[ i ]
                #/if op.target[ i ] in self.tables/else
            #/for i in range( _resultCount )
            op_index += 1
        #/for op in tableSequence
        
        # At the end, return the final tables
        
        return targetTables
    #/def process_tableSequence
    
    def process(
        self: Self,
        inputId: str,
        dfs: tuple[ pl.DataFrame, ... ] = (),
        verbose: int = 0,
        verbose_prefix: str = ''
        ) -> tuple[ pl.DataFrame,... ]:
        """
            Run inputs on the tableSequence in self.tableSequences which matches `inputIds`
        """
        if verbose > 0:
            print("# " + verbose_prefix + "Processing inputId={}".format(inputId))
        #
        return self.process_tableSequence(
            tableSequence = self.tableSequences[ inputId ],
            dfs = dfs,
            verbose = verbose,
            verbose_prefix = verbose_prefix + '  '
        )
    #/def process
#/class Web

def web_fromTable(
    table: pl.DataFrame,
    status: dict[ str, any ] = {},
    inbox: Queue | None = None,
    outbox: Queue | None = None,
    log: Queue | None = None
    ) -> CellularWeb:
    """
        :param pl.DataFrame table: A web map, with columns
            - `"cell_id": str`
            - `"cell_type": Literal[
                    'transformer',
                    'eagerMemory',
                    'patientMemory'
                ]`
            - `"cell_status": dict[ str, any ]`
            - `"table_schema": dict[ str, str ]`
            - `"update_lambda": any`
            - `"get_lambda": any`
            - `"transform_lambda": any`
            - `"destinations": list[ str ]`
            - '"input": bool'
    """
    
    cells: dict[ CellId, Cell ] = {}
    connections: dict[ CellId, CellId ] = {}
    inputIds: list[ CellId ] = []
    
    # Gather everything from the table
    for row in table.iter_rows( named = True ):
        cells[ row['cell_id'] ] = cell.fromWebRow( row )
        
        if row['destinations']:
            connections[ row['cell_id'] ] = row['destinations']
        #
        
        if row[ input ]:
            inputIds.append( row['cell_id'] )
        #
    #/for row in table.iter_rows( named = True )
    
    return CellularWeb(
        status = status,
        cells = cells,
        connections = connecctions,
        inputIds = inputIds,
        inbox = inbox,
        outbox = outbox,
        log = log
    )
#/def web_fromTable
