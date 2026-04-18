#
#//  webNu.py
#//  silverbrain
#//
#//  Created by Evan Mason on 3/11/26.

from . import tableOps, tableProcesses, types
from .schema import table_schemas, table_schemas_to_df
from .polarsDataTypeStrings import dtype_to_str
from .tableInit import (
    TableInitRef, ProcessInitRef,
    table_init_df_from_dict, process_init_df_from_dict,
    topological_init_order_from_df,
)

import polars as pl

from typing import Self

# -- System table schemas
_SYSTEM_SCHEMAS: dict[ str, dict ] = {
    '__tables_schema__':    table_schemas[ '__tables_schema__'   ],
    '__table_processes__':  table_schemas[ '__table_processes__' ],
    '__table_op_schema__':  table_schemas[ '__table_op_schema__'  ],
    '__table_op_effects__': table_schemas[ '__table_op_effects__' ],
    '__table_init__':       table_schemas[ '__table_init__'  ],
    '__process_init__':     table_schemas[ '__process_init__'],
}

# -- Built-in tableOps (shared across all Web instances)

# concat new process rows (reindexed to avoid ID collisions) onto __table_processes__
_register_process_op = tableOps.TransformOp(
    lam = lambda dfs, verbose, verbose_prefix: (
        pl.concat(
            [
                dfs[ 1 ],  # existing __table_processes__
                tableProcesses.reindex_process_df(
                    dfs[ 0 ],  # __new_process__ (serialized at start_id=0)
                    offset = int( dfs[ 1 ][ 'node_id' ].max() + 1 )
                             if dfs[ 1 ].shape[ 0 ] > 0 else 0,
                ),
            ],
            how = 'vertical',
        ),
    )
)

# concat new schema rows onto __table_op_schema__
_register_op_schema_op = tableOps.TransformOp(
    lam = lambda dfs, verbose, verbose_prefix: (
        pl.concat( [ dfs[ 1 ], dfs[ 0 ] ], how = 'vertical' ),
    )
)

# concat new effects rows onto __table_op_effects__
_register_op_effects_op = tableOps.TransformOp(
    lam = lambda dfs, verbose, verbose_prefix: (
        pl.concat( [ dfs[ 1 ], dfs[ 0 ] ], how = 'vertical' ),
    )
)

def _collect_subtree_pids(
    rows: dict,
    pid:  int,
) -> set[ int ]:
    """Return the set of all node_ids in the subtree rooted at pid."""
    pids = { pid }
    row  = rows[ pid ]
    for child in ( row.get( 'term_ids' ) or [] ):
        pids |= _collect_subtree_pids( rows, child )
    #
    if row.get( 'condition' ) is not None:
        pids |= _collect_subtree_pids( rows, row[ 'condition' ] )
    #
    for child in ( row.get( 'ifs' ) or [] ):
        pids |= _collect_subtree_pids( rows, child )
    #
    for child in ( row.get( 'thens' ) or [] ):
        pids |= _collect_subtree_pids( rows, child )
    #
    return pids
#/def _collect_subtree_pids


def _build_op_bindings(
    table_processes_df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Derive op_bindings from the serialised process tree.
    For every root process (parent_id is null, type != 'Op') walk its full
    subtree and emit one row per non-Op node that has a non-empty source.
    """
    rows      = { r[ 'node_id' ]: r for r in table_processes_df.to_dicts() }
    root_rows = table_processes_df.filter(
        pl.col( 'parent_id' ).is_null() & ( pl.col( 'type' ) != 'Op' )
    ).to_dicts()
    frames: list[ pl.DataFrame ] = []
    for root_row in root_rows:
        root_op_id = root_row[ 'op_id' ]
        for pid in _collect_subtree_pids( rows, root_row[ 'node_id' ] ):
            r = rows[ pid ]
            if r[ 'type' ] == 'Op' or not r.get( 'source' ):
                continue
            frames.append( pl.DataFrame(
                [{
                    'root_op_id': root_op_id,
                    'op_id':      r[ 'op_id' ],
                    'source':     r[ 'source' ] or [],
                    'target':     r[ 'target' ] or [],
                }],
                schema = table_schemas[ 'op_bindings' ],
            ) )
        #
    #
    return (
        pl.concat( frames, how = 'vertical' )
        if frames
        else pl.DataFrame( schema = table_schemas[ 'op_bindings' ] )
    )
#/def _build_op_bindings


# build op_bindings from __table_processes__
_compute_op_bindings_op = tableOps.TransformOp(
    lam = lambda dfs, verbose, verbose_prefix: (
        _build_op_bindings( dfs[ 0 ] ),
    )
)

_BUILTIN_OPS: types.TableProcessDict = {
    '_register_process_op':     _register_process_op,
    '_register_op_schema_op':   _register_op_schema_op,
    '_register_op_effects_op':  _register_op_effects_op,
    '_compute_op_bindings_op':  _compute_op_bindings_op,
}

# -- Built-in process definitions

_COMPUTE_OP_BINDINGS = tableProcesses.TableProcessRef(
    op_id  = 'compute_op_bindings',
    source = ( '__table_processes__', ),
    target = ( 'op_bindings', ),
    terms  = [ '_compute_op_bindings_op' ],
)

_REGISTER_PROCESS = tableProcesses.TableProcessRef(
    op_id   = 'register_process',
    source = ( '__new_process__', '__table_processes__' ),
    target = ( '__table_processes__', ),
    terms  = [ '_register_process_op' ],
)

_REGISTER_TABLEOPS = tableProcesses.TableProcessSequence(
    op_id   = 'register_tableOps',
    source = ( '__new_op_schema__', '__new_op_effects__', '__table_op_schema__', '__table_op_effects__' ),
    target = ( '__table_op_schema__', '__table_op_effects__' ),
    terms  = [
        tableProcesses.TableProcessRef(
            op_id   = '_reg_op_schema',
            source = ( '__new_op_schema__', '__table_op_schema__' ),
            target = ( '__table_op_schema__', ),
            terms  = [ '_register_op_schema_op' ],
        ),
        tableProcesses.TableProcessRef(
            op_id   = '_reg_op_effects',
            source = ( '__new_op_effects__', '__table_op_effects__' ),
            target = ( '__table_op_effects__', ),
            terms  = [ '_register_op_effects_op' ],
        ),
    ],
)


class Web:
    """
    Fresh implementation of Web with a fixed set of system tables that every
    instance carries.

    System tables (always present, named with dunder convention):
        __tables_schema__    — schema registry; one row per table
                               (table_id, column_names, column_types)
        __table_processes__  — serialized process trees; dispatched by apply_id()
        __table_op_schema__  — per-column schema declarations for registered tableOps
        __table_op_effects__ — side-effect declarations for registered tableOps
        __table_init__       — serialized TableInitRef rows
        __process_init__     — serialized ProcessInitRef rows

    All system-table schemas are themselves rows in __tables_schema__, so the
    table is self-describing from construction.

    Running:
        apply_id(op_id)  — the single dispatch entry point; looks up op_id in
                           __table_processes__, pulls inputs from self.tables,
                           runs the process, and writes outputs back to self.tables.

    Extension:
        register_tableOps(ops)    — merge ops into self.tableOps and update
                                    __table_op_schema__ / __table_op_effects__.
        register_process(process) — serialize and append a TaggedTableProcess to
                                    __table_processes__ via the staging-table pattern.
        register_tables(tables)   — merge tables into self.tables and append
                                    schema rows to __tables_schema__ for each new
                                    DataFrame.

    The built-in 'register_process' and 'register_tableOps' processes are
    themselves registered during __init__, so they are available via apply_id().
    """

    def __init__(
        self: Self,
        verbose: int = 0,
    ) -> None:
        self.verbose:  int                    = verbose
        self.tableOps: types.TableProcessDict = dict( _BUILTIN_OPS )

        # -- Initialise system tables
        self.tables: dict[ str, pl.DataFrame ] = {
            '__table_processes__':  pl.DataFrame( schema = table_schemas[ '__table_processes__'   ] ),
            '__table_op_schema__':  pl.DataFrame( schema = table_schemas[ '__table_op_schema__'    ] ),
            '__table_op_effects__': pl.DataFrame( schema = table_schemas[ '__table_op_effects__'   ] ),
            '__table_init__':       pl.DataFrame( schema = table_schemas[ '__table_init__'    ] ),
            '__process_init__':     pl.DataFrame( schema = table_schemas[ '__process_init__'  ] ),
            # __tables_schema__ bootstraps itself — its own schema is one of the rows
            '__tables_schema__':    table_schemas_to_df( _SYSTEM_SCHEMAS ),
        }

        # -- Seed __table_processes__ with built-in process definitions
        _df, _ = _REGISTER_PROCESS.as_polars( start_id = 0 )
        self.tables[ '__table_processes__' ] = pl.concat(
            [ self.tables[ '__table_processes__' ], _df ], how = 'vertical'
        )

        _next_id = int( self.tables[ '__table_processes__' ][ 'node_id' ].max() + 1 )
        _df2, _  = _REGISTER_TABLEOPS.as_polars( start_id = _next_id )
        self.tables[ '__table_processes__' ] = pl.concat(
            [ self.tables[ '__table_processes__' ], _df2 ], how = 'vertical'
        )

        _next_id = int( self.tables[ '__table_processes__' ][ 'node_id' ].max() + 1 )
        _df3, _  = _COMPUTE_OP_BINDINGS.as_polars( start_id = _next_id )
        self.tables[ '__table_processes__' ] = pl.concat(
            [ self.tables[ '__table_processes__' ], _df3 ], how = 'vertical'
        )
    #/def __init__

    # -- Dispatch

    def apply_id(
        self: Self,
        dfs: tuple[ pl.DataFrame,... ],
        op_id: str,
        processes_table_id: str = '__table_processes__',
        verbose: int = 0,
        verbose_prefix: str = '',
    ) -> tuple[ pl.DataFrame,... ]:
        """
        Stateless dispatch: look up op_id in __table_processes__, run it against
        the provided dfs, and return the result tuple.
        """
        df       = self.tables[ processes_table_id ]
        root_pid = tableProcesses.find_root_pid( df, op_id )
        return tableProcesses.run_from_df(
            df             = df,
            root_pid       = root_pid,
            dfs            = dfs,
            tableOps       = self.tableOps,
            verbose        = verbose,
            verbose_prefix = verbose_prefix,
        )
    #/def apply_id

    def run_id(
        self: Self,
        op_id: str,
        extra: dict[ str, pl.DataFrame ] | None = None,
        processes_table_id: str = '__table_processes__',
        verbose: int | None = None,
        verbose_prefix: str = '',
    ) -> None:
        """
        Stateful dispatch: pull all inputs named in source from self.tables merged
        with extra, run the process, and write outputs back to self.tables by target.
        Pass extra to inject tables not yet in self.tables (e.g. staged inputs).
        """
        _verbose   = self.verbose if verbose is None else verbose
        df         = self.tables[ processes_table_id ]
        root_pid   = tableProcesses.find_root_pid( df, op_id )
        rows       = {
            r[ 'node_id' ]: r for r in df.to_dicts()
        }
        top_row    = rows[ root_pid ]
        all_tables = dict( self.tables ) | dict( extra or {} )
        dfs        = tuple(
            all_tables[ tid ]
            for tid in top_row[ 'source' ]
        )
        result     = self.apply_id(
            dfs                 = dfs,
            op_id               = op_id,
            processes_table_id  = processes_table_id,
            verbose             = _verbose,
            verbose_prefix      = verbose_prefix,
        )
        self.tables |= dict( zip( top_row[ 'target' ], result ) )
    #/def run_id

    # -- Registration

    def register_tableOps(
        self: Self,
        ops: types.TableProcessDict,
    ) -> None:
        """
        Merge `ops` into self.tableOps and update __table_op_schema__ /
        __table_op_effects__ for any op exposing get_schema_df / get_effects_df.
        Dispatches 'register_tableOps' via apply_id(), passing new rows directly
        as dfs and writing the results back to self.tables.
        """
        self.tableOps |= ops
        _schema_rows:  list[ pl.DataFrame ] = []
        _effects_rows: list[ pl.DataFrame ] = []
        for op_id, op in ops.items():
            if hasattr( op, 'get_schema_df' ):
                _schema_rows.append( op.get_schema_df( op_id ) )
            if hasattr( op, 'get_effects_df' ):
                _effects_rows.append( op.get_effects_df( op_id ) )
        #/for op_id, op in ops.items()
        
        _new_schema  = (
            pl.concat( _schema_rows,  how = 'vertical' )
            if _schema_rows
            else pl.DataFrame( schema = table_schemas[ '__table_op_schema__' ] )
        )
        _new_effects = (
            pl.concat( _effects_rows, how = 'vertical' )
            if _effects_rows
            else pl.DataFrame( schema = table_schemas[ '__table_op_effects__' ] )
        )
        (
            self.tables[ '__table_op_schema__'  ],
            self.tables[ '__table_op_effects__' ],
        ) = self.apply_id(
            dfs = (
                _new_schema,
                _new_effects,
                self.tables[ '__table_op_schema__'  ],
                self.tables[ '__table_op_effects__' ],
            ),
            op_id = 'register_tableOps',
        )
    #/def register_tableOps

    def register_process(
        self: Self,
        process: types.TaggedTableProcess,
    ) -> None:
        """
        Serialize `process` and append it to __table_processes__.
        The new rows are serialized at start_id=0; run_id('register_process')
        reindexes them to avoid collisions before appending.

        NOTE: If you have a process already serialized as a DataFrame, you can call
        run_id( 'register_process', extra = { '__new_process__': process_df } ) directly.
        """
        _df, _ = process.as_polars( start_id = 0 )
        self.run_id(
            'register_process',
            extra = { '__new_process__': _df }
        )
    #/def register_process

    def register_tables(
        self: Self,
        tables: dict[ str, pl.DataFrame | None ],
    ) -> None:
        """
        Merge `tables` into self.tables.
        Appends a __tables_schema__ row for each new entry that is a DataFrame
        (None-valued placeholders are merged silently with no schema row).
        """
        new_rows: list[ dict ] = []
        for tid, df in tables.items():
            if isinstance( df, pl.DataFrame ) and tid not in self.tables:
                new_rows.append({
                    'table_id':     tid,
                    'column_names': list( df.columns ),
                    'column_types': [ dtype_to_str( df.schema[ c ] ) for c in df.columns ],
                })
            #/if isinstance( df, pl.DataFrame ) and tid not in self.tables
        #/for tid, df
        self.tables |= tables
        if new_rows:
            self.tables[ '__tables_schema__' ] = pl.concat(
                [
                    self.tables[ '__tables_schema__' ],
                    pl.DataFrame( new_rows, schema = table_schemas[ '__tables_schema__' ] ),
                ],
                how = 'vertical',
            )
        #/if new_rows
    #/def register_tables

    def _register_process_df(
        self: Self,
        process: types.TaggedTableProcess,
    ) -> None:
        """Alias for register_process(); maintains API compatibility with web.Web."""
        self.register_process( process )
    #/def _register_process_df

    def init_data(
        self: Self,
        verbose: int = 0,
        verbose_prefix: str = '',
    ) -> None:
        """
        Run all registered init ops in topological dependency order.
        For each entry in topological order derived from __table_init__ and
        __process_init__:
          - table:   run_id( ref.op_id )   if always_run or table not yet populated
          - process: run_id( ref.factory_id ) if process not yet in __table_processes__
        Factory processes are TaggedTableProcesses already registered in
        __table_processes__; when run they produce and register the target process.
        """
        for _type, _init_id in topological_init_order_from_df(
            self.tables[ '__table_init__'   ],
            self.tables[ '__process_init__' ],
        ):
            if _type == 'table':
                row = self.tables[ '__table_init__' ].filter(
                    pl.col( 'table_id' ) == _init_id
                ).row( 0, named = True )
                if row[ 'always_run' ] or self.tables.get( row[ 'table_id' ] ) is None:
                    self.run_id(
                        row[ 'op_id' ],
                        verbose        = verbose,
                        verbose_prefix = verbose_prefix,
                    )
                #
            else:
                row = self.tables[ '__process_init__' ].filter(
                    pl.col( 'op_id' ) == _init_id
                ).row( 0, named = True )
                if self.tables[ '__table_processes__' ].filter(
                    ( pl.col( 'op_id' ) == row[ 'op_id' ] )
                    & pl.col( 'parent_id' ).is_null()
                ).is_empty():
                    self.run_id(
                        row[ 'factory_id' ],
                        verbose        = verbose,
                        verbose_prefix = verbose_prefix,
                    )
                #
            #
        #/for _type, _init_id

        self.run_id( 'compute_op_bindings' )
    #/def init_data
#/class Web
